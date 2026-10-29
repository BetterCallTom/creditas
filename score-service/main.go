package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"credita/shared/config"
	"credita/shared/httpx"
	"credita/shared/logging"
	"credita/shared/telemetry"
)

const serviceName = "score-service"

type configuration struct {
	port             string
	logLevel         string
	otelEndpoint     string
	databaseURL      string
	redisURL         string
	cacheTTL         time.Duration
	artificialLatency time.Duration
	shutdownTimeout  time.Duration
}

func loadConfiguration() configuration {
	return configuration{
		port:              config.String("PORT", "8080"),
		logLevel:          config.String("LOG_LEVEL", "info"),
		otelEndpoint:      config.String("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		databaseURL:       config.String("DATABASE_URL", "postgres://credita:credita@postgres:5432/credita?sslmode=disable"),
		redisURL:          config.String("REDIS_URL", "redis://redis:6379/0"),
		cacheTTL:          config.Duration("CACHE_TTL", 5*time.Minute),
		artificialLatency: config.Duration("SCORE_LATENCY", 0),
		shutdownTimeout:   config.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

type server struct {
	logger         *slog.Logger
	settings       configuration
	pool           *pgxpool.Pool
	cache          *redis.Client
	scoreRequests  *prometheus.CounterVec
	burnDuration   prometheus.Histogram
}

type scoreResponse struct {
	CPF    string `json:"cpf"`
	Valor  int    `json:"valor"`
	Faixa  string `json:"faixa"`
	Origem string `json:"origem"`
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

	pool, err := pgxpool.New(context.Background(), settings.databaseURL)
	if err != nil {
		logger.Error("falha ao conectar no postgres", slog.String("error", err.Error()))
		return
	}
	defer pool.Close()

	redisOptions, err := redis.ParseURL(settings.redisURL)
	if err != nil {
		logger.Error("url do redis invalida", slog.String("error", err.Error()))
		return
	}
	cache := redis.NewClient(redisOptions)
	defer cache.Close()

	application := &server{
		logger:   logger,
		settings: settings,
		pool:     pool,
		cache:    cache,
		scoreRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "score_requests_total",
			Help:        "Total de consultas de score por origem do dado.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"origem"}),
		burnDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:        "score_burn_duration_seconds",
			Help:        "Duracao efetiva das chamadas ao endpoint de burn de CPU.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: prometheus.Labels{"service": serviceName},
		}),
	}

	metrics := httpx.NewHTTPMetrics(serviceName)

	router := chi.NewRouter()
	router.Get("/healthz", httpx.Healthz)
	router.Get("/readyz", httpx.Readyz(application.checkPostgres, application.checkRedis))
	router.Handle("/metrics", promhttp.Handler())
	router.Group(func(instrumented chi.Router) {
		instrumented.Use(metrics.Middleware)
		instrumented.Get("/score/{cpf}", application.handleScore)
		instrumented.Get("/burn", application.handleBurn)
	})

	handler := httpx.Instrument(serviceName, router)
	if err := httpx.Run(logger, ":"+settings.port, handler, settings.shutdownTimeout); err != nil {
		logger.Error("servidor encerrado com erro", slog.String("error", err.Error()))
	}
}

func (s *server) handleScore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cpf := chi.URLParam(r, "cpf")

	if s.settings.artificialLatency > 0 {
		injectLatency(ctx, s.settings.artificialLatency)
	}

	if valor, err := s.readFromCache(ctx, cpf); err == nil {
		s.scoreRequests.WithLabelValues("cache").Inc()
		httpx.WriteJSON(w, http.StatusOK, scoreResponse{CPF: cpf, Valor: valor, Faixa: faixa(valor), Origem: "cache"})
		return
	}

	valor, found, err := s.readFromDatabase(ctx, cpf)
	if err != nil {
		s.logger.Error("falha ao ler score no banco", slog.String("cpf", cpf), slog.String("error", err.Error()))
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "falha ao consultar score"})
		return
	}

	origem := "database"
	if !found {
		valor = deterministicScore(cpf)
		origem = "calculado"
		if err := s.persistScore(ctx, cpf, valor); err != nil {
			s.logger.Warn("falha ao persistir score calculado", slog.String("cpf", cpf), slog.String("error", err.Error()))
		}
	}

	s.writeToCache(ctx, cpf, valor)
	s.scoreRequests.WithLabelValues(origem).Inc()
	httpx.WriteJSON(w, http.StatusOK, scoreResponse{CPF: cpf, Valor: valor, Faixa: faixa(valor), Origem: origem})
}

func (s *server) handleBurn(w http.ResponseWriter, r *http.Request) {
	milliseconds := 100
	if raw := r.URL.Query().Get("ms"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			milliseconds = parsed
		}
	}
	if milliseconds > 60000 {
		milliseconds = 60000
	}

	duration := time.Duration(milliseconds) * time.Millisecond
	_, span := telemetry.Tracer(serviceName).Start(r.Context(), "burn_cpu")
	start := time.Now()
	burnCPU(duration)
	elapsed := time.Since(start)
	span.End()

	s.burnDuration.Observe(elapsed.Seconds())
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"queimado_ms": milliseconds,
		"decorrido_ms": elapsed.Milliseconds(),
	})
}

func (s *server) readFromCache(ctx context.Context, cpf string) (int, error) {
	ctx, span := telemetry.Tracer(serviceName).Start(ctx, "redis_get")
	defer span.End()
	raw, err := s.cache.Get(ctx, cacheKey(cpf)).Result()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(raw)
}

func (s *server) writeToCache(ctx context.Context, cpf string, valor int) {
	ctx, span := telemetry.Tracer(serviceName).Start(ctx, "redis_set")
	defer span.End()
	if err := s.cache.Set(ctx, cacheKey(cpf), valor, s.settings.cacheTTL).Err(); err != nil {
		s.logger.Warn("falha ao gravar cache", slog.String("cpf", cpf), slog.String("error", err.Error()))
	}
}

func (s *server) readFromDatabase(ctx context.Context, cpf string) (int, bool, error) {
	ctx, span := telemetry.Tracer(serviceName).Start(ctx, "postgres_select_score")
	defer span.End()
	var valor int
	err := s.pool.QueryRow(ctx, "SELECT valor FROM score WHERE cpf = $1", cpf).Scan(&valor)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return valor, true, nil
}

func (s *server) persistScore(ctx context.Context, cpf string, valor int) error {
	ctx, span := telemetry.Tracer(serviceName).Start(ctx, "postgres_insert_score")
	defer span.End()
	_, err := s.pool.Exec(ctx,
		"INSERT INTO score (cpf, valor, faixa) VALUES ($1, $2, $3) ON CONFLICT (cpf) DO UPDATE SET valor = EXCLUDED.valor, faixa = EXCLUDED.faixa, atualizado_em = now()",
		cpf, valor, faixa(valor),
	)
	return err
}

func (s *server) checkPostgres(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *server) checkRedis(ctx context.Context) error {
	return s.cache.Ping(ctx).Err()
}

func injectLatency(ctx context.Context, duration time.Duration) {
	_, span := telemetry.Tracer(serviceName).Start(ctx, "latencia_artificial")
	defer span.End()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func burnCPU(duration time.Duration) {
	deadline := time.Now().Add(duration)
	accumulator := 0.0
	for time.Now().Before(deadline) {
		for index := 0; index < 10000; index++ {
			accumulator += math.Sqrt(float64(index) + accumulator)
		}
	}
	cpuSink = accumulator
}

var cpuSink float64

func deterministicScore(cpf string) int {
	digest := sha256.Sum256([]byte(cpf))
	value := binary.BigEndian.Uint32(digest[:4])
	return 300 + int(value%701)
}

func faixa(valor int) string {
	switch {
	case valor >= 800:
		return "excelente"
	case valor >= 600:
		return "bom"
	case valor >= 450:
		return "regular"
	default:
		return "baixo"
	}
}

func cacheKey(cpf string) string {
	return "score:" + cpf
}
