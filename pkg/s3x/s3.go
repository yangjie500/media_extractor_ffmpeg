package s3x

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	S3 *s3.Client
}

func New(ctx context.Context, region string, opts ...func(*config.LoadOptions) error) (*Client, error) {
	lo := []func(*config.LoadOptions) error{}
	if region != "" {
		lo = append(lo, config.WithRegion(region))
	}
	lo = append(lo, opts...)

	cfg, err := config.LoadDefaultConfig(ctx, lo...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &Client{S3: s3.NewFromConfig(cfg)}, nil
}

func (c *Client) GetObjectToWriter(ctx context.Context, bucket, key string, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	out, err := c.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("s3 get %s/%s: %w", bucket, key, err)
	}

	if _, err := io.Copy(w, out.Body); err != nil {
		return fmt.Errorf("stream copy: %w", err)
	}

	return nil
}

func (c *Client) PutObjectFromFile(ctx context.Context, bucket, key string, filepath, contentType string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	}

	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	if _, err := c.S3.PutObject(ctx, input); err != nil {
		return fmt.Errorf("s3 put %s/%s: %w", bucket, key, err)
	}

	return nil

}
