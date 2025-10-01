package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/yangjie500/media_extractor_ffmpeg/internal/producer"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
)

func main() {
	count := flag.Int("n", 3, "number of sample messages to produce")
	flag.Parse()

	cfg, err := config.LoadAll("configs/.env.production")
	if err != nil {
		logger.Errorf("config error: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := producer.ProduceSample(ctx, cfg, *count); err != nil {
		logger.Errorf("produce failed: %v", err)
		os.Exit(1)
	}
	logger.Infof("done")
	fmt.Println("OK")
}
