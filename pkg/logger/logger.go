package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/yangjie500/media_extractor_ffmpeg/pkg/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Level int32

const (
	Debug Level = iota
	Info
	Warn
	Error
)

var (
	currentLevel atomic.Int32
	// logger       = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	logger *log.Logger // initialized in init()
)

func init() {
	initWriters()
	SetLevelFromEnv()
}

func initWriters() {
	_, err := config.LoadAll("configs/.env.production")
	if err != nil {
		fmt.Errorf("Error reading configuration")
	}
	logFile := os.Getenv("LOG_FILE")
	toStdout := os.Getenv("LOG_TO_STDOUT") == "true"
	maxSizeMB := mustInt("LOG_MAX_SIZE_MB", 100) // rotate at 100MB
	maxBackups := mustInt("LOG_MAX_BACKUPS", 7)  // keep 7 old files
	maxAgeDays := mustInt("LOG_MAX_AGE_DAYS", 14)
	compress := os.Getenv("LOG_COMPRESS") == "true"
	fmt.Println(logFile)
	rot := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    maxSizeMB,
		MaxBackups: maxBackups,
		MaxAge:     maxAgeDays,
		Compress:   compress,
	}

	var w io.Writer = rot
	if toStdout {
		w = io.MultiWriter(os.Stdout, rot)
	}
	// Use UTC timestamps for consistency across hosts/regions.
	flags := log.LstdFlags | log.Lmicroseconds | log.Lshortfile | log.LUTC
	logger = log.New(w, "", flags)
}

func mustInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func SetLevelFromEnv() {
	levelStr := strings.ToLower(os.Getenv("LOG_LEVEL"))
	switch levelStr {
	case "debug":
		SetLevel(Debug)
	case "info":
		SetLevel(Info)
	case "warn":
		SetLevel(Warn)
	case "error":
		SetLevel(Error)
	default:
		SetLevel(Info)
	}
}

func SetLevel(l Level) {
	currentLevel.Store(int32(l))
}

func logf(l Level, prefix, format string, args ...interface{}) {
	if l < Level(currentLevel.Load()) {
		return
	}

	msg := fmt.Sprintf(format, args...)
	logger.Output(4, fmt.Sprintf("[%s] %s", prefix, msg))
}

func Debugf(format string, args ...interface{}) {
	logf(Debug, "DEBUG", format, args...)
}

func Infof(format string, args ...interface{}) {
	logf(Info, "INFO", format, args...)
}

func Warnf(format string, args ...interface{}) {
	logf(Warn, "WARN", format, args...)
}

func Errorf(format string, args ...interface{}) {
	logf(Error, "ERROR", format, args...)
}
