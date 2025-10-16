package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/ffmpegx"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/s3x"
)

type MergeRequest struct {
	MediaKey string `json:"media_id"`

	VideoBucket string `json:"video_bucket"`
	VideoKey    string `json:"video_key"`
	AudioBucket string `json:"audio_bucket"`
	AudioKey    string `json:"audio_key"`
	VideoID     string `json:"video_id"`
	AudioID     string `json:"audio_id"`
	Region      string `json:"region"`

	CorrelationID string `json:"correlation_id,omitempty"`
	OutputBucket  string `json:"output_bucket,omitempty"` // default: VideoBucket
	OutputKey     string `json:"output_key,omitempty"`    // default: derived from VideoKey
}

type MergeResult struct {
	Status        string  `json:"status"`
	VideoID       string  `json:"video_id"`
	OutputBucket  string  `json:"output_bucket,omitempty"`
	OutputKey     string  `json:"output_key,omitempty"`
	ETag          string  `json:"etag,omitempty"`
	SizeBytes     int64   `json:"size_bytes,omitempty"`
	DurationSec   float64 `json:"duration_sec,omitempty"`
	CorrelationID string  `json:"correlation_id,omitempty"`
	Error         string  `json:"err,omitempty"`
}

type Service struct {
	cfg          config.Config
	resultWriter *kafka.Writer
}

func NewService(cfg config.Config) *Service {
	var w *kafka.Writer

	if cfg.KafkaProducerTopic != "" {
		w = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  cfg.KafkaProducerBroker,
			Topic:    cfg.KafkaProducerTopic,
			Balancer: &kafka.LeastBytes{},
		})
	}

	return &Service{cfg: cfg, resultWriter: w}
}

func (s *Service) Close() {
	if s.resultWriter != nil {
		_ = s.resultWriter.Close()
	}
}

func (s *Service) HandleMessage(ctx context.Context, key, value []byte) error {
	var req MergeRequest
	if err := json.Unmarshal(value, &req); err != nil {
		return fmt.Errorf("parse merge request: %w", err)
	}

	region := req.Region
	if region == "" {
		region = s.cfg.Region
	}
	if region == "" {
		return fmt.Errorf("missing AWS region (event.Region empty and AWS_REGION not configured)")
	}

	// Derive output location if missing
	outBucket := req.OutputBucket
	if outBucket == "" {
		outBucket = req.VideoBucket
	}
	outKey := req.OutputKey
	if outKey == "" {
		outKey = deriveOutputKey(req.VideoKey)
	}

	// Work dir (unique per job)
	jobDir, err := os.MkdirTemp("./tmp", "merge-*")
	if err != nil {
		return fmt.Errorf("mktemp: %w", err)
	}
	defer os.RemoveAll(jobDir)

	videoPath := filepath.Join(jobDir, "video_in.mp4")
	audioPath := filepath.Join(jobDir, "audio_in.m4a")
	mergedPath := filepath.Join(jobDir, "merged_out.mp4")

	s3c, err := s3x.New(ctx, region)
	if err != nil {
		return fmt.Errorf("s3 init: %w", err)
	}

	// Download inputs
	logger.Infof("downloading s3://%s/%s", req.VideoBucket, req.VideoKey)
	vf, err := os.Create(videoPath)
	if err != nil {
		return fmt.Errorf("create video file: %w", err)
	}
	if err := s3c.GetObjectToWriter(ctx, req.VideoBucket, req.VideoKey, vf); err != nil {
		vf.Close()
		return fmt.Errorf("download video: %w", err)
	}
	vf.Close()

	logger.Infof("downloading s3://%s/%s", req.AudioBucket, req.AudioKey)
	af, err := os.Create(audioPath)
	if err != nil {
		return fmt.Errorf("create audio file: %w", err)
	}
	if err := s3c.GetObjectToWriter(ctx, req.AudioBucket, req.AudioKey, af); err != nil {
		af.Close()
		return fmt.Errorf("download audio: %w", err)
	}
	af.Close()

	// Merge with ffmpeg
	logger.Infof("merging -> %s", mergedPath)
	if err := ffmpegx.MergeAV(ctx, videoPath, audioPath, mergedPath, "aac"); err != nil {
		// Emit failure result
		return err
	}

	// Probe output for duration (optional)
	si, _ := ffmpegx.Probe(ctx, mergedPath)

	// Upload merged
	logger.Infof("uploading s3://%s/%s", outBucket, outKey)
	if err := s3c.PutObjectFromFile(ctx, outBucket, outKey, mergedPath, "video/mp4"); err != nil {
		return fmt.Errorf("upload merged: %w", err)
	}

	res := MergeResult{
		Status:        "merged",
		VideoID:       req.VideoID,
		OutputBucket:  outBucket,
		OutputKey:     outKey,
		DurationSec:   si.Duration,
		CorrelationID: req.CorrelationID,
	}

	if err := s.emitResult(ctx, res); err != nil {
		logger.Warnf("emit result failed: %v", err)
	}

	logger.Infof("merge completed: s3://%s/%s (duration=%.2fs)", outBucket, outKey, si.Duration)
	return nil

}

func (s *Service) emitResult(ctx context.Context, res MergeResult) error {
	if s.resultWriter == nil {
		return nil // No output topic configured; it's OK to skip emitting
	}

	val, _ := json.Marshal(res)
	key := []byte(res.VideoID)
	return s.resultWriter.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: val,
		Time:  time.Now().UTC(),
	})

}

func deriveOutputKey(videoKey string) string {
	if videoKey == "" {
		return "video_merged.mp4"
	}

	dir := filepath.Dir(videoKey)
	base := filepath.Base(videoKey)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	return filepath.Join(dir, name+"_merged.mp4")
}
