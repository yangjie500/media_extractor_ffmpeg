package s3c

import (
	"context"
	"fmt"
	"io"
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
