package main

import (
	"context"
	"os"

	s3c "github.com/yangjie500/media_extractor_ffmpeg/pkg/aws"
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

	region := os.Getenv("AWS_REGION") // e.g., "ap-southeast-1"
	ctx := context.Background()
	s3client, err := s3c.New(ctx, region)
	if err != nil {
		logger.Errorf("init s3: %v", err)
		return
	}

	bucket := "media-extractor"
	key := "testkey.png"
	// --- Or stream to a file ---
	f, _ := os.Create("downloaded.txt")
	defer f.Close()
	if err := s3client.GetObjectToWriter(ctx, bucket, key, f); err != nil {
		logger.Errorf("download to file: %v", err)
		return
	}
	logger.Infof("wrote downloaded.txt")
}
