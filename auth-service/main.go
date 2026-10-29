package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"credita/shared/config"
	"credita/shared/httpx"
	"credita/shared/logging"
	"credita/shared/telemetry"
)

const serviceName = "auth-service"

type configuration struct {
	port            string
	logLevel        string
	otelEndpoint    string
	jwtSecret       string
	tokenTTL        time.Duration
	shutdownTimeout time.Duration
}

func loadConfiguration() configuration {
	return configuration{
		port:            config.String("PORT", "8080"),
		logLevel:        config.String("LOG_LEVEL", "info"),
		otelEndpoint:    config.String("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		jwtSecret:       config.String("JWT_SECRET", "credita-dev-secret"),
		tokenTTL:        config.Duration("JWT_TTL", time.Hour),
		shutdownTimeout: config.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

type server struct {
	logger        *slog.Logger
	settings      configuration
	loginCounter  *prometheus.CounterVec
	validateTotal *prometheus.CounterVec
}

type loginRequest struct {
	CPF      string `json:"cpf"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
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

	application := &server{
		logger:   logger,
		settings: settings,
		loginCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "auth_login_total",
			Help:        "Total de tentativas de login por resultado.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"result"}),
		validateTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "auth_validate_total",
			Help:        "Total de validacoes de token por resultado.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"result"}),
	}

	metrics := httpx.NewHTTPMetrics(serviceName)

	router := chi.NewRouter()
	router.Get("/healthz", httpx.Healthz)
	router.Get("/readyz", httpx.Readyz())
	router.Handle("/metrics", promhttp.Handler())
	router.Group(func(instrumented chi.Router) {
		instrumented.Use(metrics.Middleware)
		instrumented.Post("/login", application.handleLogin)
		instrumented.Get("/validate", application.handleValidate)
	})

	handler := httpx.Instrument(serviceName, router)
	if err := httpx.Run(logger, ":"+settings.port, handler, settings.shutdownTimeout); err != nil {
		logger.Error("servidor encerrado com erro", slog.String("error", err.Error()))
	}
}

func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var request loginRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.CPF == "" {
		s.loginCounter.WithLabelValues("invalid").Inc()
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "cpf obrigatorio"})
		return
	}

	expiresAt := time.Now().Add(s.settings.tokenTTL)
	claims := jwt.MapClaims{
		"sub": request.CPF,
		"iss": "credita-auth",
		"iat": time.Now().Unix(),
		"exp": expiresAt.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(s.settings.jwtSecret))
	if err != nil {
		s.loginCounter.WithLabelValues("error").Inc()
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "falha ao emitir token"})
		return
	}

	s.loginCounter.WithLabelValues("success").Inc()
	httpx.WriteJSON(w, http.StatusOK, loginResponse{Token: signed, ExpiresAt: expiresAt.Unix()})
}

func (s *server) handleValidate(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if raw == "" {
		s.validateTotal.WithLabelValues("missing").Inc()
		httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "token ausente"})
		return
	}

	parsed, err := jwt.Parse(raw, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(s.settings.jwtSecret), nil
	})
	if err != nil || !parsed.Valid {
		s.validateTotal.WithLabelValues("invalid").Inc()
		httpx.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "token invalido"})
		return
	}

	claims, _ := parsed.Claims.(jwt.MapClaims)
	s.validateTotal.WithLabelValues("valid").Inc()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"valid":   true,
		"subject": claims["sub"],
	})
}
