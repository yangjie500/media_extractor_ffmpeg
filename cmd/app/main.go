package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/yangjie500/media_extractor_ffmpeg/internal/consumer"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/kafkautil"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
)

func main() {
	cfg, err := config.LoadAll("configs/.env.production")
	if err != nil {
		logger.Errorf("config: %v", err)
	}
	logger.Infof("loaded config")

	// Create topic if not exists
	if cfg.AutoCreateTopics {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if cfg.KafkaProducerTopic != "" {
			if err := kafkautil.EnsureTopicWithRetry(ctx, cfg.KafkaBrokers,
				cfg.KafkaProducerTopic,
				cfg.KafkaOutputTopicPartitions,
				cfg.KafkaOutputTopicReplication,
				nil, 5, 300*time.Millisecond); err != nil {
				logger.Warnf("ensure output topic %q: %v", cfg.KafkaProducerTopic, err)
			} else {
				logger.Infof("output topic ready: %s", cfg.KafkaProducerTopic)
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Infof("consumer starting topic=%s group=%s brokers=%v", cfg.KafkaTopic, cfg.KafkaGroupId, cfg.KafkaBrokers)
	if err := consumer.Start(ctx, cfg); err != nil && err != context.Canceled {
		logger.Errorf("consumer stopped with error: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	logger.Infof("consumer exited")
}
