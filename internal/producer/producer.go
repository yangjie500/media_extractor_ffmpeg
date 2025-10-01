package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
)

// VideoAudioEvent matches your consumer's expected schema.
type VideoAudioEvent struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Source    string          `json:"source"`
	Timestamp time.Time       `json:"ts"`
	Payload   json.RawMessage `json:"payload"`
}

func ProduceSample(ctx context.Context, cfg config.Config, n int) error {
	w := newWriter(cfg)
	defer w.Close()

	samplePayload := map[string]interface{}{
		"name":   fmt.Sprintf("user-%d", rand.Intn(100)),
		"active": rand.Intn(2) == 1,
		"score":  rand.Float64() * 100,
	}

	// Marshal to JSON string
	jsonBytes, err := json.Marshal(samplePayload)
	if err != nil {
		panic(err)
	}

	for i := 1; i <= n; i++ {
		now := time.Now().UTC()
		e := VideoAudioEvent{
			ID:        fmt.Sprintf("ev-%d", i),
			Type:      "UPLOAD_COMPLETED",
			Source:    "producer-cli",
			Timestamp: now,
			Payload:   json.RawMessage(jsonBytes),
		}

		value, _ := json.Marshal(e)

		msg := kafka.Message{
			Key:   []byte(e.ID),
			Value: value,
			Time:  now,
		}

		if err := w.WriteMessages(ctx, msg); err != nil {
			return fmt.Errorf("write message %d: %w", i, err)
		}

	}
	return nil
}

func newWriter(cfg config.Config) *kafka.Writer {
	wc := kafka.WriterConfig{
		Topic:    cfg.KafkaTopic,
		Brokers:  cfg.KafkaBrokers,
		Balancer: &kafka.LeastBytes{},
	}

	return kafka.NewWriter(wc)
}
