package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/yangjie500/media_extractor_ffmpeg/internal/consumer"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
)

func main() {
	// Load from .env + environment
	cfg, err := config.LoadAll("configs/.env.production")
	if err != nil {
		logger.Errorf("config error: %v", err)
		return
	}

	logger.Infof("loaded config: %s", cfg.AppName)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Infof("consumer starting topic=%s group=%s brokers=%v", cfg.KafkaTopic, cfg.KafkaGroupId, cfg.KafkaBrokers)
	if err := consumer.Start(ctx, cfg); err != nil && err != context.Canceled {
		logger.Errorf("consumer stopped with error: %v", err)
	}

	// small grace
	time.Sleep(150 * time.Millisecond)
	logger.Infof("consumer exited")

	// region := os.Getenv("AWS_REGION") // e.g., "ap-southeast-1"
	// ctx := context.Background()
	// s3client, err := s3c.New(ctx, region)
	// if err != nil {
	// 	logger.Errorf("init s3: %v", err)
	// 	return
	// }

	// bucket := "media-extractor"
	// key := "testkey.png"
	// --- Or stream to a file ---
	// f, _ := os.Create("downloaded.txt")
	// defer f.Close()
	// if err := s3client.GetObjectToWriter(ctx, bucket, key, f); err != nil {
	// 	logger.Errorf("download to file: %v", err)
	// 	return
	// }
	// logger.Infof("wrote downloaded.txt")

	// bucket = "media-extractor"
	// key = "vulnerability.csv"
	// if err := s3client.PutObjectFromFile(ctx, bucket, key, "./vulnerability.csv", "text/csv"); err != nil {
	// 	logger.Errorf("Failed to upload: %v", err)
	// 	return
	// }
	// logger.Infof("Uploaded to bucket")

}
