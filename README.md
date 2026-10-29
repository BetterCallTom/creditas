# Credita

Fatia (~1/10) de uma fintech de crédito brasileira, feita como **base realista
para praticar Kubernetes**. O foco não é a lógica de negócio: é expor tudo que
uma stack K8s de produção consome — métricas, traces distribuídos, health checks,
eventos assíncronos e carga.

> **Este repositório entrega SOMENTE a aplicação.** Não há nenhum artefato de
> Kubernetes (manifests, Helm, Kustomize, Ingress, cert-manager). A camada K8s é
> escrita à mão, de propósito, por quem usa este projeto.

## Stack

Go 1.23 · chi · pgx · go-redis · segmentio/kafka-go · OpenTelemetry (OTLP) ·
Prometheus client · slog (JSON) · Postgres · Redis · Kafka (KRaft).

## Serviços

Monorepo, um módulo Go (`credita`), um diretório por serviço e um `shared/` com
as libs comuns (config, logging, telemetria, middleware HTTP, trace de Kafka).

| Serviço | Papel | Rotas de negócio | Dependências |
|---|---|---|---|
| `gateway` | Reverse proxy / BFF. Roteia e propaga `traceparent`. Sem lógica própria. | `/login`, `/validate`, `/score/*`, `/burn`, `/consulta/*` | auth, score, consulta |
| `auth-service` | Emite e valida JWT (fake). | `POST /login`, `GET /validate` | — |
| `score-service` | Score determinístico 300–1000 por CPF. Cache no Redis, miss no Postgres. | `GET /score/{cpf}`, `GET /burn?ms=N` | Postgres, Redis |
| `consulta-service` | Lê cadastro no Postgres e publica evento no Kafka a cada consulta. | `GET /consulta/{cpf}` | Postgres, Kafka |
| `notification-worker` | Consome o tópico `consultas` e processa com delay configurável. Sem HTTP de negócio. | — (só health/metrics) | Kafka |

Todos os serviços expõem, na mesma porta:

- `GET /healthz` — liveness, sempre `200` enquanto o processo vive.
- `GET /readyz` — readiness, `200` só quando as dependências respondem (DB/Redis/Kafka).
- `GET /metrics` — formato Prometheus.

## Observabilidade

- **Métricas HTTP** em todo serviço: `http_requests_total`,
  `http_request_duration_seconds` (histograma), com labels `method/route/status`.
- **Métricas de negócio**: `score_requests_total{origem}`,
  `score_burn_duration_seconds`, `consultas_total{resultado}`,
  `consultas_eventos_publicados_total`, `worker_messages_processed_total`,
  `worker_queue_lag`, `auth_login_total`, `auth_validate_total`.
- **Traces distribuídos** via OTLP, propagação W3C `traceparent`:
  - `gateway -> score -> (redis_get | postgres_select_score)`
  - `gateway -> consulta -> kafka_publish_consultas -> (worker) processa_consulta`
  - O contexto atravessa HTTP (otelhttp) e Kafka (headers injetados/extraídos).

Se `OTEL_EXPORTER_OTLP_ENDPOINT` estiver vazio, o exporter vira no-op (o app sobe
sem collector). No K8s, aponte para o OTel Collector / Tempo.

## Configuração (12-factor, tudo por env)

| Variável | Serviços | Padrão |
|---|---|---|
| `PORT` | todos | `8080` |
| `LOG_LEVEL` | todos | `info` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | todos | vazio (desliga export) |
| `SHUTDOWN_TIMEOUT` | todos | `15s` |
| `AUTH_SERVICE_URL` / `SCORE_SERVICE_URL` / `CONSULTA_SERVICE_URL` | gateway | `http://<svc>:8080` |
| `JWT_SECRET` / `JWT_TTL` | auth | `credita-dev-secret` / `1h` |
| `DATABASE_URL` | score, consulta | `postgres://credita:credita@postgres:5432/credita?sslmode=disable` |
| `REDIS_URL` | score | `redis://redis:6379/0` |
| `CACHE_TTL` | score | `5m` |
| `SCORE_LATENCY` | score | `0` (latência artificial, ex: `250ms`) |
| `KAFKA_BROKERS` / `KAFKA_TOPIC` | consulta, worker | `kafka:9092` / `consultas` |
| `KAFKA_GROUP` | worker | `notification-worker` |
| `WORKER_PROCESS_DELAY` | worker | `500ms` (atrasa o processamento p/ acumular lag) |

## Rodando local (sanity)

Pré-requisitos: Docker + Docker Compose. (Opcional: Go 1.23 e k6.)

```bash
cd credita
go mod tidy        # gera go.sum (necessário para o build das imagens)
docker compose up --build
```

A ordem é garantida por healthcheck: Postgres/Redis/Kafka sobem, `migrate` roda
as migrations, e só então score/consulta sobem.

Testes rápidos:

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/login -d '{"cpf":"11111111111","password":"x"}'
curl -s localhost:8080/score/11111111111
curl -s localhost:8080/consulta/11111111111
curl -s "localhost:8080/burn?ms=300"
curl -s localhost:8080/metrics | head
```

> O `docker-compose.yml` é **só para sanity local** — confirmar que o app sobe
> antes de levar pro K8s. Não é topologia de produção.

## Migrations

Em `migrations/`, no formato golang-migrate (`up`/`down`). Tabelas
`consulta_cadastral` e `score`, com 5 CPFs fake pré-populados (`11111111111` …
`55555555555`). No compose são aplicadas pelo serviço `migrate`.

## Carga (k6)

```bash
BASE_URL=http://localhost:8080 k6 run load/k6-script.js
```

Ramp-up até 150 VUs misturando `/score`, `/consulta` e `/burn`, com thresholds de
p95. Use para forçar CPU (HPA), latência (tracing) e lag na fila (KEDA).

## Mapa: feature do app → recurso de Kubernetes

| Feature do app | O que exercita no K8s |
|---|---|
| `GET /burn?ms=N` (CPU controlada) | **HPA por CPU** (`resources.requests.cpu` + HPA) |
| `worker` + lag do Kafka (`worker_queue_lag`, `WORKER_PROCESS_DELAY`) | **KEDA** (ScaledObject por lag de consumer group) |
| `GET /readyz` (checa DB/Redis/Kafka) | **readinessProbe** (tira o pod do Service até estar pronto) |
| `GET /healthz` (liveness) | **livenessProbe** |
| `SIGTERM` + graceful shutdown (drena conexões) | **rolling update**, `terminationGracePeriodSeconds`, **PodDisruptionBudget** |
| `GET /metrics` (Prometheus) | **ServiceMonitor / PodMonitor** (Prometheus Operator) |
| OTLP + `traceparent` fim a fim | **OTel Collector** + Tempo/Jaeger |
| Config 100% por env | **ConfigMap** / **Secret** / `envFrom` |
| Imagem distroless, non-root, sem shell | **Pod Security** (`restricted`), scan Trivy, assinatura de imagem |
| `gateway` roteando por caminho | **Ingress** / Gateway API (escrito à mão) |
| `score`/`consulta` lendo Postgres e Redis | **StatefulSet** / serviços gerenciados + **NetworkPolicy** |
| `SCORE_LATENCY` (latência artificial) | visualizar p95 em traces e tunar probes/HPA |

## Notas

- **Nenhuma dependência de cloud provider.**
- O código evita comentários por escolha de estilo; rode `gofmt -w .` e
  `go vet ./...` após `go mod tidy`.
- JWT, score e dados cadastrais são **fake** — isto é um laboratório.
