package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
)

type MergeRequest struct {
	VideoBucket   string `json:"video_bucket"`
	VideoKey      string `json:"video_key"`
	AudioBucket   string `json:"audio_bucket"`
	AudioKey      string `json:"audio_key"`
	VideoID       string `json:"video_id"`
	AudioID       string `json:"audio_id"`
	Region        string `json:"region"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func main() {
	// Load .env → brokers/topic from your existing config package
	cfg, err := config.LoadAll("configs/.env.production")
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	req := MergeRequest{
		VideoBucket:   "media-extractor",
		VideoKey:      "connor/video.mp4",
		AudioBucket:   "media-extractor",
		AudioKey:      "connor/audio.m4a",
		VideoID:       "vid-123",
		AudioID:       "aud-456",
		Region:        "ap-southeast-1",
		CorrelationID: "req-abc-001",
	}

	val, _ := json.Marshal(req)

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic, // your input topic from .env (e.g., merge.requests)
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg := kafka.Message{
		Key:   []byte(req.VideoID), // partition key (useful for ordering)
		Value: val,
		Time:  time.Now().UTC(),
	}

	if err := w.WriteMessages(ctx, msg); err != nil {
		log.Fatalf("write: %v", err)
	}
	log.Println("sent test message:", string(val))
}
