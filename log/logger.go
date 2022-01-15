package log

import (
	"errors"
	"log"
	"os"
)

var debugLogger *log.Logger = log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
var infoLogger *log.Logger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
var warningLogger *log.Logger = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
var errorLogger *log.Logger = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

var logger = "debug"
var isDebugEnabled = false
var isInfoEnabled = true
var isWarningEnabled = true
var isErrorEnabled = true

func SetLogger(logger string) (err error) {
	if logger == "debug" {
		isDebugEnabled = true
	} else if logger == "info" {
		isDebugEnabled = false
		isInfoEnabled = true
	} else if logger == "warning" {
		isDebugEnabled = false
		isInfoEnabled = false
		isWarningEnabled = true
	} else if logger == "error" {
		isDebugEnabled = false
		isInfoEnabled = false
		isWarningEnabled = false
		isErrorEnabled = true
	} else {
		err = errors.New("invalid format")
	}
	return
}

func Debug(format string, args ...interface{}) {
	if isDebugEnabled {
		debugLogger.Printf(format, args...)
	}
}

func Info(format string, args ...interface{}) {
	if isInfoEnabled {
		infoLogger.Printf(format, args...)
	}
}

func Warn(format string, args ...interface{}) {
	if isWarningEnabled {
		warningLogger.Printf(format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if isErrorEnabled {
		errorLogger.Printf(format, args...)
	}
}
