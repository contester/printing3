package main

import (
	"code.google.com/p/log4go"
	"github.com/contester/printing3/tools"
	"github.com/contester/printing3/tickets"
	"flag"
	"code.google.com/p/goconf/conf"
	"github.com/jjeffery/stomp"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"path/filepath"
	"io/ioutil"
	"os"
	"os/exec"
)

type server struct {
	*tools.StompConfig
	Workdir, Gsprint string
}

func (s *server) processIncoming(conn *stomp.Conn, msg *stomp.Message) error {
	var job tickets.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		return err
	}

	sourceName := fmt.Sprintf("%s.ps", job.GetJobId())
	sourceFullName := filepath.Join(s.Workdir, sourceName)
	if buf, err := job.Data.Bytes(); err == nil {
		if err = ioutil.WriteFile(sourceFullName, buf, os.ModePerm); err != nil {
			return err
		}
	} else {
		return err
	}

	cmd := exec.Command(s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	stompSpec := flag.String("messaging", "", "")

	flag.Parse()

	var srv server
	srv.Gsprint = "gsprint.exe"
	srv.Workdir = os.TempDir()

	if *configFileName != "" {
		config, err := conf.ReadConfigFile(*configFileName)
		if err != nil {
			log4go.Error("Reading config file: %s", err)
			return
		}

		if s, err := config.GetString("server", "stomp"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			*stompSpec = s
		}
		if s, err := config.GetString("directories", "temp"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			srv.Workdir = s
		}
	}
	var err error
	srv.StompConfig, err = tools.ParseStompDSN(*stompSpec)
	if err != nil {
		return
	}

	srv.ReceiveLoop("print", srv.processIncoming)
}
