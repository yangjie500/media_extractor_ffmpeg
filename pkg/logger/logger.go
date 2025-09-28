package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
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
	logger       = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
)

func init() {
	SetLevelFromEnv()
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
