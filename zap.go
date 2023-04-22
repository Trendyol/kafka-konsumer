package kafka

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logger struct {
	*zap.SugaredLogger
}

func NewZapLogger(level LogLevel) LoggerInterface {
	if level == "" {
		level = LogLevelInfo
	}

	l, _ := newLogger(level)
	return &logger{l.Sugar()}
}

func (l *logger) With(args ...interface{}) LoggerInterface {
	if len(args) > 0 {
		return &logger{l.SugaredLogger.With(args...)}
	}
	return l
}

func newLogger(level LogLevel) (*zap.Logger, error) {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "sourceLocation",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05.999Z"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// default log level is Info
	lvl := zapcore.InfoLevel
	_ = lvl.Set(string(level))

	const initial = 100
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(lvl),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    initial,
			Thereafter: initial,
		},
		Encoding:         "json",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	return config.Build(zap.AddStacktrace(zap.FatalLevel))
}
