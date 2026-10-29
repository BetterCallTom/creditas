package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

const serviceName = "consulta-service"

type configuration struct {
	port            string
	logLevel        string
	otelEndpoint    string
	databaseURL     string
	kafkaBrokers    string
	kafkaTopic      string
	shutdownTimeout time.Duration
}

func loadConfiguration() configuration {
	return configuration{
		port:            config.String("PORT", "8080"),
		logLevel:        config.String("LOG_LEVEL", "info"),
		otelEndpoint:    config.String("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		databaseURL:     config.String("DATABASE_URL", "postgres://credita:credita@postgres:5432/credita?sslmode=disable"),
		kafkaBrokers:    config.String("KAFKA_BROKERS", "kafka:9092"),
		kafkaTopic:      config.String("KAFKA_TOPIC", "consultas"),
		shutdownTimeout: config.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

type server struct {
	logger         *slog.Logger
	settings       configuration
	pool           *pgxpool.Pool
	writer         *kafka.Writer
	consultasTotal *prometheus.CounterVec
	eventosTotal   *prometheus.CounterVec
}

type cadastro struct {
	CPF            string `json:"cpf"`
	Nome           string `json:"nome"`
	DataNascimento string `json:"data_nascimento"`
	Cidade         string `json:"cidade"`
	UF             string `json:"uf"`
	Situacao       string `json:"situacao"`
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

	pool, err := pgxpool.New(context.Background(), settings.databaseURL)
	if err != nil {
		logger.Error("falha ao conectar no postgres", slog.String("error", err.Error()))
		return
	}
	defer pool.Close()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(settings.kafkaBrokers),
		Topic:                  settings.kafkaTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireOne,
	}
	defer writer.Close()

	application := &server{
		logger:   logger,
		settings: settings,
		pool:     pool,
		writer:   writer,
		consultasTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "consultas_total",
			Help:        "Total de consultas cadastrais por resultado.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"resultado"}),
		eventosTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "consultas_eventos_publicados_total",
			Help:        "Total de eventos publicados no Kafka por resultado.",
			ConstLabels: prometheus.Labels{"service": serviceName},
		}, []string{"resultado"}),
	}

	metrics := httpx.NewHTTPMetrics(serviceName)

	router := chi.NewRouter()
	router.Get("/healthz", httpx.Healthz)
	router.Get("/readyz", httpx.Readyz(application.checkPostgres, application.checkKafka))
	router.Handle("/metrics", promhttp.Handler())
	router.Group(func(instrumented chi.Router) {
		instrumented.Use(metrics.Middleware)
		instrumented.Get("/consulta/{cpf}", application.handleConsulta)
	})

	handler := httpx.Instrument(serviceName, router)
	if err := httpx.Run(logger, ":"+settings.port, handler, settings.shutdownTimeout); err != nil {
		logger.Error("servidor encerrado com erro", slog.String("error", err.Error()))
	}
}

func (s *server) handleConsulta(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cpf := chi.URLParam(r, "cpf")

	registro, found, err := s.readCadastro(ctx, cpf)
	if err != nil {
		s.consultasTotal.WithLabelValues("erro").Inc()
		s.logger.Error("falha ao consultar cadastro", slog.String("cpf", cpf), slog.String("error", err.Error()))
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "falha ao consultar cadastro"})
		return
	}
	if !found {
		s.consultasTotal.WithLabelValues("nao_encontrado").Inc()
		httpx.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "cpf nao encontrado"})
		return
	}

	s.consultasTotal.WithLabelValues("encontrado").Inc()
	s.publishEvent(ctx, consultaEvent{CPF: cpf, Situacao: registro.Situacao, ConsultaEm: time.Now().UTC()})
	httpx.WriteJSON(w, http.StatusOK, registro)
}

func (s *server) readCadastro(ctx context.Context, cpf string) (cadastro, bool, error) {
	ctx, span := telemetry.Tracer(serviceName).Start(ctx, "postgres_select_cadastro")
	defer span.End()

	var registro cadastro
	var nascimento time.Time
	err := s.pool.QueryRow(ctx,
		"SELECT cpf, nome, data_nascimento, cidade, uf, situacao FROM consulta_cadastral WHERE cpf = $1", cpf,
	).Scan(&registro.CPF, &registro.Nome, &nascimento, &registro.Cidade, &registro.UF, &registro.Situacao)
	if errors.Is(err, pgx.ErrNoRows) {
		return cadastro{}, false, nil
	}
	if err != nil {
		return cadastro{}, false, err
	}
	registro.DataNascimento = nascimento.Format("2006-01-02")
	return registro, true, nil
}

func (s *server) publishEvent(ctx context.Context, event consultaEvent) {
	ctx, span := telemetry.Tracer(serviceName).Start(ctx, "kafka_publish_consultas")
	defer span.End()

	payload, err := json.Marshal(event)
	if err != nil {
		s.eventosTotal.WithLabelValues("erro").Inc()
		s.logger.Error("falha ao serializar evento", slog.String("error", err.Error()))
		return
	}

	headers := []kafka.Header{}
	kafkax.Inject(ctx, &headers)

	if err := s.writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(event.CPF),
		Value:   payload,
		Headers: headers,
	}); err != nil {
		s.eventosTotal.WithLabelValues("erro").Inc()
		s.logger.Error("falha ao publicar evento", slog.String("error", err.Error()))
		return
	}
	s.eventosTotal.WithLabelValues("sucesso").Inc()
}

func (s *server) checkPostgres(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *server) checkKafka(ctx context.Context) error {
	connection, err := kafka.DialContext(ctx, "tcp", s.settings.kafkaBrokers)
	if err != nil {
		return err
	}
	defer connection.Close()
	_, err = connection.Brokers()
	return err
}
