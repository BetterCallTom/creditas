package kafkax

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

type headerCarrier struct {
	headers *[]kafka.Header
}

func (c headerCarrier) Get(key string) string {
	for _, header := range *c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	updated := make([]kafka.Header, 0, len(*c.headers)+1)
	for _, header := range *c.headers {
		if header.Key != key {
			updated = append(updated, header)
		}
	}
	updated = append(updated, kafka.Header{Key: key, Value: []byte(value)})
	*c.headers = updated
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

func Inject(ctx context.Context, headers *[]kafka.Header) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: headers})
}

func Extract(ctx context.Context, headers []kafka.Header) context.Context {
	local := headers
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &local})
}
