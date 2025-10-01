package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// App
	AppName string
	Env     string

	// AWS
	Region string

	// Kafka
	KafkaBrokers     []string
	KafkaTopic       string
	KafkaGroupId     string
	KafkaClientId    string
	KafkaStartOffset string
	KafkaMinBytes    int
	KafkaMaxBytes    int
	KafkaMaxWait     time.Duration
	KafkaCommitEvery time.Duration

	// Kafka
	KafkaProducerBroker []string
	KafkaProducerTopic  string
}

func LoadAll(dotenvPaths ...string) (Config, error) {
	// Load first .env found (if exists). Safe to call with no args, it tries ".env" by default.
	if len(dotenvPaths) > 0 {
		_ = godotenv.Load(dotenvPaths...)
	} else {
		_ = godotenv.Load()
	}

	return Load()
}

func Load() (Config, error) {
	var cfg Config
	var errs []string

	cfg.AppName = getenv("APP_NAME", "APP_NEEDS_NAME")
	cfg.Env = getenv("APP_ENV", "DEVELOPMENT")

	// --- AWS ---
	cfg.Region = getenv("AWS_REGION", "ap-southeast-1")

	// --- Kafka required ---
	brokers := getenv("KAFKA_BROKERS", "")
	if brokers == "" {
		errs = append(errs, "KAFKA_BROKERS is required (comma-separated)")
	} else {
		cfg.KafkaBrokers = splitAndTrim(brokers, ",")
	}
	cfg.KafkaTopic = getenv("KAFKA_TOPIC", "")
	if cfg.KafkaTopic == "" {
		errs = append(errs, "KAFKA_TOPIC is required")
	}
	cfg.KafkaGroupId = getenv("KAFKA_GROUP_ID", "")
	if cfg.KafkaGroupId == "" {
		errs = append(errs, "KAFKA_GROUP_ID is required")
	}

	// --- Kafka tuning ---
	cfg.KafkaClientId = getenv("KAFKA_CLIENT_ID", "myapp")
	cfg.KafkaMinBytes = mustInt("KAFKA_MIN_BYTES", 1, &errs)
	cfg.KafkaMaxBytes = mustInt("KAFKA_MAX_BYTES", 1048576, &errs)
	cfg.KafkaMaxWait = mustDuration("KAFKA_MAX_WAIT", 250*time.Millisecond, &errs)
	cfg.KafkaCommitEvery = mustDuration("KAFKA_COMMIT_INTERVAL", 0, &errs)
	cfg.KafkaStartOffset = strings.ToLower(getenv("KAFKA_START_OFFSET", "last"))

	// --- Kafka Producer ---
	KafkaProducerBroker := getenv("KAFKA_BROKERS_PRODUCER", "")
	if brokers == "" {
		errs = append(errs, "KAFKA_BROKERS_PRODUCER is required (comma-separated)")
	} else {
		cfg.KafkaProducerBroker = splitAndTrim(KafkaProducerBroker, ",")
	}
	cfg.KafkaProducerTopic = getenv("KAFKA_TOPIC_PRODUCER", "")
	if cfg.KafkaProducerTopic == "" {
		errs = append(errs, "KAFKA_TOPIC_PRODUCER is required")
	}

	if len(errs) > 0 {
		return cfg, errors.New(strings.Join(errs, "; "))
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustInt(key string, def int, errs *[]string) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, key+": invalid int ("+err.Error()+")")
		return def
	}
	return n
}

func mustDuration(key string, def time.Duration, errs *[]string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, key+": invalid duration ("+err.Error()+")")
		return def
	}
	return d
}

func splitAndTrim(s, sep string) []string {
	raw := strings.Split(s, sep)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
