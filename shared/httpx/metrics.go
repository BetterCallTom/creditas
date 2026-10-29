package httpx

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type HTTPMetrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

func NewHTTPMetrics(serviceName string) *HTTPMetrics {
	labels := prometheus.Labels{"service": serviceName}
	return &HTTPMetrics{
		requests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "Total de requisicoes HTTP processadas.",
			ConstLabels: labels,
		}, []string{"method", "route", "status"}),
		latency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "Duracao das requisicoes HTTP em segundos.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: labels,
		}, []string{"method", "route", "status"}),
	}
}

func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		route := routePattern(r)
		status := strconv.Itoa(recorder.status)
		m.requests.WithLabelValues(r.Method, route, status).Inc()
		m.latency.WithLabelValues(r.Method, route, status).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(status int) {
	if !s.wroteHeader {
		s.status = status
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(status)
}

func (s *statusRecorder) Write(data []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(data)
}

func routePattern(r *http.Request) string {
	if ctx := chi.RouteContext(r.Context()); ctx != nil {
		if pattern := ctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return r.URL.Path
}
