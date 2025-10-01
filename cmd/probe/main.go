package main

import (
	"context"
	"time"

	"github.com/yangjie500/media_extractor_ffmpeg/pkg/ffmpegx"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
)

func main() {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := ffmpegx.Probe(ctx, "connor.mp4")
	if err != nil {
		logger.Errorf("%v", err)
	}

	logger.Infof("%+v\n", result)

	err = ffmpegx.MergeAV(ctx, "./connor.mp4", "./connor.m4a", "./connor-merged.mp4", "aac")
	if err != nil {
		logger.Errorf("%v", err)
	}

}
