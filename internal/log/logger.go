package log

import (
	"errors"
	"flag"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

const DebugFormatName string = "debug"
const InfoFormatName string = "info"
const WarningFormatName string = "warning"
const ErrorFormatName string = "error"

var debugLogger *log.Logger = log.New(pqLogWriter{}, "DEBUG: ", 0)
var infoLogger *log.Logger = log.New(pqLogWriter{}, "INFO: ", 0)
var warningLogger *log.Logger = log.New(pqLogWriter{}, "WARNING: ", 0)
var errorLogger *log.Logger = log.New(pqLogWriter{}, "ERROR: ", 0)

var isDebugEnabled = true
var isInfoEnabled = true
var isWarningEnabled = true
var isErrorEnabled = true

func init() {
	if flag.Lookup("test.v") != nil {
		SetLogger("debug")
		isDebugEnabled = true
	}
}

// SetLogger sets logging level for logger
func SetLogger(logger string) (err error) {
	if logger == DebugFormatName {
		isDebugEnabled = true
		isInfoEnabled = true
		isWarningEnabled = true
		isErrorEnabled = true
	} else if logger == InfoFormatName {
		isDebugEnabled = false
		isInfoEnabled = true
		isWarningEnabled = true
		isErrorEnabled = true
	} else if logger == WarningFormatName {
		isDebugEnabled = false
		isInfoEnabled = false
		isWarningEnabled = true
		isErrorEnabled = true
	} else if logger == ErrorFormatName {
		isDebugEnabled = false
		isInfoEnabled = false
		isWarningEnabled = false
		isErrorEnabled = true
	} else {
		err = errors.New("invalid format - " + logger)
	}
	return
}

// Debugf print debug any
func Debugf(format string, args ...any) {
	if isDebugEnabled {
		debugLogger.Printf(format, args...)
	}
}

// Debug print debug any
func Debug(args ...any) {
	if isDebugEnabled {
		debugLogger.Print(args...)
	}
}

// Infof print info any
func Infof(format string, args ...any) {
	if isInfoEnabled {
		infoLogger.Printf(format, args...)
	}
}

// Info print info any
func Info(args ...any) {
	if isInfoEnabled {
		infoLogger.Print(args...)
	}
}

// Warnf print warn any
func Warnf(format string, args ...any) {
	if isWarningEnabled {
		warningLogger.Printf(format, args...)
	}
}

// Warn print warn any
func Warn(args ...any) {
	if isWarningEnabled {
		warningLogger.Print(args...)
	}
}

// Error print error any
func Error(args ...any) {
	if isErrorEnabled {
		errorLogger.Print(args...)
	}
}

// Errorf print error any with format
func Errorf(format string, args ...any) {
	if isErrorEnabled {
		errorLogger.Printf(format, args...)
	}
}

type pqLogWriter struct{}

func (f pqLogWriter) Write(p []byte) (n int, err error) {

	pc, file, line, ok := runtime.Caller(4)
	if !ok {
		file = "?"
		line = 0
	}

	fn := runtime.FuncForPC(pc)
	var fnName string
	if fn == nil {
		fnName = "?()"
	} else {
		dotName := filepath.Ext(fn.Name())
		fnName = strings.TrimLeft(dotName, ".") + "()"
	}

	log.Printf("%s:%d %s: %s", filepath.Base(file), line, fnName, p)
	return len(p), nil
}
