package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	// App
	AppName string
	Env     string
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
	// var errs []string

	cfg.AppName = getenv("APP_NAME", "APP_NEEDS_NAME")
	cfg.Env = getenv("APP_ENV", "DEVELOPMENT")

	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
