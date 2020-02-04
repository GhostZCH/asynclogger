package asynclogger

import (
	"bytes"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var empty []byte = []byte("")

var levels = map[string]zapcore.Level{
	"info":  zap.InfoLevel,
	"warn":  zap.WarnLevel,
	"error": zap.ErrorLevel}

type Conf struct {
	Path      string
	MaxSize   int
	BufLimit  int
	QueueSize int
	Level     string
	ZapConf   zapcore.EncoderConfig
}

type Logger struct {
	queue      chan []byte
	flush      bool
	conf       *Conf
	Zap        *zap.Logger
	Lumberjack *lumberjack.Logger
}

func (l *Logger) Sync() error {
	l.flush = true
	return nil
}

func (l *Logger) Write(p []byte) (n int, err error) {
	tmp := make([]byte, len(p))
	copy(tmp, p)
	l.queue <- tmp
	return len(tmp), nil
}

func (l *Logger) run() {
	b := make([][]byte, 0)
	for msg := range l.queue {
		b = append(b, msg)
		if l.flush || len(b) > l.conf.BufLimit {
			l.Lumberjack.Write(bytes.Join(b, empty))
			b = make([][]byte, 0)
			l.flush = false
		}
	}
}

func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.Zap.Info(msg, fields...)
}

func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.Zap.Warn(msg, fields...)
}

func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.Zap.Error(msg, fields...)
}

func (l *Logger) Rotate() error {
	return l.Lumberjack.Rotate()
}

func NewLogger(conf *Conf) *Logger {
	lj := &lumberjack.Logger{
		Filename:  conf.Path,
		MaxSize:   conf.MaxSize,
		MaxAge:    300,
		LocalTime: true,
		Compress:  true}

	log := &Logger{
		flush:      false,
		conf:       conf,
		queue:      make(chan []byte, conf.QueueSize),
		Lumberjack: lj,
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(conf.ZapConf),
		log,
		levels[conf.Level],
	)

	if conf.ZapConf.CallerKey != "" {
		log.Zap = zap.New(core, zap.AddCaller())
	} else {
		log.Zap = zap.New(core)
	}

	go log.run()

	return log
}
