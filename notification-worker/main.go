package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"credita/shared/config"
	"credita/shared/httpx"
	"credita/shared/kafkax"
	"credita/shared/logging"
	"credita/shared/telemetry"
)

const serviceName = "notification-worker"

type configuration struct {
	port            string
	logLevel        string
	otelEndpoint    string
	kafkaBrokers    string
	kafkaTopic      string
	kafkaGroup      string
	processDelay    time.Duration
	shutdownTimeout time.Duration
}

func loadConfiguration() configuration {
	return configuration{
		port:            config.String("PORT", "8080"),
		logLevel:        config.String("LOG_LEVEL", "info"),
		otelEndpoint:    config.String("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		kafkaBrokers:    config.String("KAFKA_BROKERS", "kafka:9092"),
		kafkaTopic:      config.String("KAFKA_TOPIC", "consultas"),
		kafkaGroup:      config.String("KAFKA_GROUP", "notification-worker"),
		processDelay:    config.Duration("WORKER_PROCESS_DELAY", 500*time.Millisecond),
		shutdownTimeout: config.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

type consultaEvent struct {
	CPF        string    `json:"cpf"`
	Situacao   string    `json:"situacao"`
	ConsultaEm time.Time `json:"consulta_em"`
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

	messagesProcessed := promauto.NewCounter(prometheus.CounterOpts{
		Name:        "worker_messages_processed_total",
		Help:        "Total de mensagens processadas pelo worker.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	})
	queueLag := promauto.NewGauge(prometheus.GaugeOpts{
		Name:        "worker_queue_lag",
		Help:        "Lag atual do consumer group no topico consumido.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	})

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{settings.kafkaBrokers},
		Topic:   settings.kafkaTopic,
		GroupID: settings.kafkaGroup,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go runHealthServer(ctx, logger, settings, reader)
	go publishLag(ctx, reader, queueLag)

	logger.Info("worker iniciado, consumindo topico", slog.String("topic", settings.kafkaTopic))
	consume(ctx, logger, reader, settings.processDelay, messagesProcessed)

	logger.Info("sinal de shutdown recebido, encerrando consumo")
}

func consume(ctx context.Context, logger *slog.Logger, reader *kafka.Reader, delay time.Duration, processed prometheus.Counter) {
	tracer := telemetry.Tracer(serviceName)
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			logger.Error("falha ao ler mensagem", slog.String("error", err.Error()))
			continue
		}

		messageCtx := kafkax.Extract(ctx, message.Headers)
		messageCtx, span := tracer.Start(messageCtx, "processa_consulta")

		var event consultaEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			logger.Warn("mensagem invalida descartada", slog.String("error", err.Error()))
		} else {
			process(messageCtx, logger, event, delay)
			processed.Inc()
		}
		span.End()

		if err := reader.CommitMessages(ctx, message); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("falha ao confirmar offset", slog.String("error", err.Error()))
		}
	}
}

func process(ctx context.Context, logger *slog.Logger, event consultaEvent, delay time.Duration) {
	_, span := telemetry.Tracer(serviceName).Start(ctx, "notifica_cliente")
	defer span.End()

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return
	}
	logger.Info("consulta processada", slog.String("cpf", event.CPF), slog.String("situacao", event.Situacao))
}

func publishLag(ctx context.Context, reader *kafka.Reader, gauge prometheus.Gauge) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gauge.Set(float64(reader.Stats().Lag))
		}
	}
}

func runHealthServer(ctx context.Context, logger *slog.Logger, settings configuration, reader *kafka.Reader) {
	router := chi.NewRouter()
	router.Get("/healthz", httpx.Healthz)
	router.Get("/readyz", httpx.Readyz(func(checkCtx context.Context) error {
		connection, err := kafka.DialContext(checkCtx, "tcp", settings.kafkaBrokers)
		if err != nil {
			return err
		}
		defer connection.Close()
		_, err = connection.Brokers()
		return err
	}))
	router.Handle("/metrics", promhttp.Handler())

	server := &http.Server{Addr: ":" + settings.port, Handler: router}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), settings.shutdownTimeout)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()

	logger.Info("servidor de health/metrics iniciado", slog.String("port", settings.port))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("servidor de health encerrado com erro", slog.String("error", err.Error()))
	}
}
