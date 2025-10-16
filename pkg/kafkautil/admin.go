package kafkautil

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/yangjie500/media_extractor_ffmpeg/pkg/logger"
)

func EnsureTopicWithRetry(
	ctx context.Context,
	brokers []string,
	topic string,
	nPart, rf int,
	cfg map[string]string,
	attempts int,
	backoff time.Duration,
) error {
	var err error
	for i := 1; i <= attempts; i++ {
		err = EnsureTopic(ctx, brokers, topic, nPart, rf, cfg)
		if err == nil {
			return nil
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// EnsureTopic checks if a topic exists; if missing, creates it using the cluster controller.
func EnsureTopic(ctx context.Context,
	brokers []string,
	topic string,
	numPartitions,
	replicationFactor int,
	configs map[string]string) error {

	if topic == "" {
		return fmt.Errorf("topic name is empty")
	}
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	exists, err := topicExists(ctx, brokers[0], topic)
	if err != nil {
		return fmt.Errorf("topicExists: %w", err)
	}
	if exists {
		return nil
	}

	// connect to controller to create topic
	ctrlAddr, err := controllerAddr(ctx, brokers[0])
	if err != nil {
		return fmt.Errorf("controllerAddr: %w", err)
	}
	conn, err := kafka.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", ctrlAddr, err)
	}
	defer conn.Close()

	logger.Warnf("partition %d", numPartitions)
	tc := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}
	if err := conn.CreateTopics(tc); err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}

	return nil

}

func topicExists(ctx context.Context, brokerAddr, topic string) (bool, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return false, fmt.Errorf("dial broker %s: %w", brokerAddr, err)
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions()
	if err != nil {
		return false, fmt.Errorf("read partitions: %w", err)
	}
	for _, p := range parts {
		if p.Topic == topic {
			return true, nil
		}
	}
	return false, nil
}

func controllerAddr(ctx context.Context, brokerAddr string) (string, error) {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return "", fmt.Errorf("dial broker %s: %w", brokerAddr, err)
	}
	defer conn.Close()

	c, err := conn.Controller()
	if err != nil {
		return "", fmt.Errorf("controller: %w", err)
	}

	host := c.Host
	port := strconv.Itoa(c.Port)

	return net.JoinHostPort(host, port), nil
}
