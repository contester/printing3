package main

import (
	"code.google.com/p/log4go"
	"github.com/contester/printing3/tools"
	"github.com/contester/printing3/tickets"
	"flag"
	"github.com/jjeffery/stomp"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"path/filepath"
	"io/ioutil"
	"os"
	"os/exec"
        "time"
)

type server struct {
	*tools.StompConfig
	Workdir, Gsprint, Queue string
}

func (s *server) processIncoming(conn *stomp.Conn, msg *stomp.Message) error {
	var job tickets.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
                log4go.Error("Received malformed job: %s", err)
		return err
	}

	sourceName := fmt.Sprintf("%s-%s.ps", time.Now().Format(time.RFC3339), job.GetJobId())
	sourceFullName := filepath.Join(s.Workdir, sourceName)
	if buf, err := job.Data.Bytes(); err == nil {
		if err = ioutil.WriteFile(sourceFullName, buf, os.ModePerm); err != nil {
	                log4go.Error("Error writing file: %s", err)
			return err
		}
	} else {
                log4go.Error("Error getting buffer: %s", err)
		return err
	}

	log4go.Info("Sending job %s to printer %s", job.GetJobId(), job.GetPrinter())
	cmd := exec.Command(s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	if err := cmd.Run(); err != nil {
                log4go.Error("Error printing: %s", err)
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
    srv.Queue = "/amq/queue/printer"

	config, err := tools.MaybeReadConfigFile(*configFileName)

	if config != nil {
		if s, err := config.GetString("server", "stomp"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			*stompSpec = s
		}
		if s, err := config.GetString("server", "queue"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			srv.Queue = s
		}
		if s, err := config.GetString("directories", "temp"); err == nil {
			log4go.Trace("Imported temp dir from config file: %s", s)
			srv.Workdir = s
		}
	}

	srv.StompConfig, err = tools.ParseStompFlagOrConfig(*stompSpec, config, "messaging")
	if err != nil {
		return
	}

	for {
		srv.ReceiveLoop(srv.Queue, srv.processIncoming)
		time.Sleep(15 * time.Second)
	}
}
