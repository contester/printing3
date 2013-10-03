package tools

import (
	"code.google.com/p/log4go"
	"log"
)

type lwrapper struct{}

func (lw *lwrapper) Write(p []byte) (n int, err error) {
	log4go.Log(log4go.ERROR, "compat", string(p))
	return n, nil
}

func SetupLogWrapper() {
	// log4go.Global = log4go.NewDefaultLogger(log4go.FINEST)
	lw := &lwrapper{}
	log.SetOutput(lw)
}

func SetupLogFile(name string) {
	log4go.Global.AddFilter("log", log4go.FINE, log4go.NewFileLogWriter(name, true))
}

func SetupLog(name string) {
	SetupLogFile(name)
	SetupLogWrapper()
}
