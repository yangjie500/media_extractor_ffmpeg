package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
)

func Start(ctx context.Context, cfg config.Config) error {
	r := newReader(cfg)
	defer func() {
		if err := r.Close(); err != nil {
			logger.Warnf("reader close error: %v", err)
		}
	}()

	svc := NewService(cfg)
	defer svc.Close()

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			logger.Warnf("fetch error: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		logger.Debugf("Fetched partition=%d offet=%d key%q", msg.Partition, msg.Offset, string(msg.Key))

		var hErr error
		for attempt := 1; attempt <= 3; attempt++ {
			hErr = svc.HandleMessage(ctx, msg.Key, msg.Value)
			if hErr == nil {
				break
			}
			logger.Warnf("Handle failed attempt=%d offset=%d err=%v", attempt, msg.Offset, hErr)
			select {
			case <-time.After(time.Duration(attempt) * 300 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if hErr != nil {
			// Consider producing to a DLQ here
			logger.Errorf("dropping message after retries offset=%d err=%v", msg.Offset, hErr)
			// (we still commit to avoid blocking the partition)
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			logger.Warnf("commit failed offset=%d err=%v", msg.Offset, err)
		} else {
			logger.Debugf("committed offset=%d", msg.Offset)
		}

	}
}

func newReader(cfg config.Config) *kafka.Reader {
	rc := kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		GroupID:        cfg.KafkaGroupId,
		Topic:          cfg.KafkaTopic,
		MinBytes:       cfg.KafkaMinBytes,
		MaxBytes:       cfg.KafkaMaxBytes,
		MaxWait:        cfg.KafkaMaxWait,
		CommitInterval: cfg.KafkaCommitEvery,
	}

	switch cfg.KafkaStartOffset {
	case "first":
		rc.StartOffset = kafka.FirstOffset
	default:
		rc.StartOffset = kafka.LastOffset
	}

	// Optional TLS/SASL
	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: cfg.KafkaClientId,
	}
	rc.Dialer = dialer
	return kafka.NewReader(rc)
}
