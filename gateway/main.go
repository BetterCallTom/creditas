package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"credita/shared/config"
	"credita/shared/httpx"
	"credita/shared/logging"
	"credita/shared/telemetry"
)

const serviceName = "gateway"

type configuration struct {
	port            string
	logLevel        string
	otelEndpoint    string
	authURL         string
	scoreURL        string
	consultaURL     string
	clientTimeout   time.Duration
	shutdownTimeout time.Duration
}

func loadConfiguration() configuration {
	return configuration{
		port:            config.String("PORT", "8080"),
		logLevel:        config.String("LOG_LEVEL", "info"),
		otelEndpoint:    config.String("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		authURL:         config.String("AUTH_SERVICE_URL", "http://auth-service:8080"),
		scoreURL:        config.String("SCORE_SERVICE_URL", "http://score-service:8080"),
		consultaURL:     config.String("CONSULTA_SERVICE_URL", "http://consulta-service:8080"),
		clientTimeout:   config.Duration("CLIENT_TIMEOUT", 5*time.Second),
		shutdownTimeout: config.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

func main() {
	settings := loadConfiguration()
	logger := logging.New(serviceName, settings.logLevel)

	shutdownTelemetry, err := telemetry.Setup(context.Background(), serviceName, settings.otelEndpoint)
	if err != nil {
		logger.Error("falha ao iniciar telemetria", slog.String("error", err.Error()))
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownTelemetry(ctx)
	}()

	metrics := httpx.NewHTTPMetrics(serviceName)
	healthClient := httpx.NewClient(settings.clientTimeout)

	authProxy := newProxy(logger, settings.authURL)
	scoreProxy := newProxy(logger, settings.scoreURL)
	consultaProxy := newProxy(logger, settings.consultaURL)

	router := chi.NewRouter()
	router.Get("/healthz", httpx.Healthz)
	router.Get("/readyz", httpx.Readyz(
		dependencyCheck(healthClient, settings.authURL),
		dependencyCheck(healthClient, settings.scoreURL),
		dependencyCheck(healthClient, settings.consultaURL),
	))
	router.Handle("/metrics", promhttp.Handler())

	router.Group(func(proxied chi.Router) {
		proxied.Use(metrics.Middleware)
		proxied.Handle("/login", authProxy)
		proxied.Handle("/validate", authProxy)
		proxied.Handle("/score/*", scoreProxy)
		proxied.Handle("/burn", scoreProxy)
		proxied.Handle("/consulta/*", consultaProxy)
	})

	handler := httpx.Instrument(serviceName, router)
	if err := httpx.Run(logger, ":"+settings.port, handler, settings.shutdownTimeout); err != nil {
		logger.Error("servidor encerrado com erro", slog.String("error", err.Error()))
	}
}

func newProxy(logger *slog.Logger, target string) *httputil.ReverseProxy {
	parsed, err := url.Parse(target)
	if err != nil {
		logger.Error("url de destino invalida", slog.String("target", target), slog.String("error", err.Error()))
	}
	proxy := httputil.NewSingleHostReverseProxy(parsed)
	proxy.Transport = otelhttp.NewTransport(http.DefaultTransport)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logger.Error("falha ao encaminhar requisicao", slog.String("target", target), slog.String("error", err.Error()))
		httpx.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": "servico indisponivel"})
	}
	return proxy
}

func dependencyCheck(client *http.Client, target string) httpx.ReadinessCheck {
	return func(ctx context.Context) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target+"/healthz", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return &dependencyError{target: target, status: response.StatusCode}
		}
		return nil
	}
}

type dependencyError struct {
	target string
	status int
}

func (e *dependencyError) Error() string {
	return e.target + " respondeu status " + http.StatusText(e.status)
}
