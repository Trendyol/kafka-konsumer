package logger

type Level string

const (
	Debug Level = "debug"
	Info  Level = "info"
	Warn  Level = "warn"
	Error Level = "error"
)

// Interface is a logger that supports log levels, context and structured logging.
type Interface interface {
	// With returns a logger based off the root logger and decorates it with the given context and arguments.
	With(args ...interface{}) Interface

	// Debug uses fmt.Sprint to construct and log a message at DEBUG level
	Debug(args ...interface{})
	// Info uses fmt.Sprint to construct and log a message at INFO level
	Info(args ...interface{})
	// Warn uses fmt.Sprint to construct and log a message at ERROR level
	Warn(args ...interface{})
	// Error uses fmt.Sprint to construct and log a message at ERROR level
	Error(args ...interface{})

	// Debugf uses fmt.Sprintf to construct and log a message at DEBUG level
	Debugf(format string, args ...interface{})
	// Infof uses fmt.Sprintf to construct and log a message at INFO level
	Infof(format string, args ...interface{})
	// Warnf uses fmt.Sprintf to construct and log a message at WARN level
	Warnf(format string, args ...interface{})
	// Errorf uses fmt.Sprintf to construct and log a message at ERROR level
	Errorf(format string, args ...interface{})

	Infow(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
}
