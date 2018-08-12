package main

import (
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

type server struct {
	Workdir, Gsprint string
}

func (s *server) processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
	var job tickets.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		log.Errorf("Received malformed job: %s", err)
		return err
	}

	sourceName := time.Now().Format("2006-01-02T15-04-05") + "-" + job.GetJobId() + ".ps"
	sourceFullName := filepath.Join(s.Workdir, sourceName)
	if buf, err := job.Data.Bytes(); err == nil {
		if err = ioutil.WriteFile(sourceFullName, buf, os.ModePerm); err != nil {
			log.Errorf("Error writing file: %s", err)
			return err
		}
	} else {
		log.Printf("Error getting buffer: %s", err)
		return err
	}

	log.Infof("Sending job %s to printer %s", job.GetJobId(), job.GetPrinter())
	if tools.DryRun() {
		log.Infof("Would run: %q %s %q %q", s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
		return nil
	}
	cmd := exec.Command(s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		log.Errorf("Error printing: %s", err)
		return err
	}

	return nil
}

func main() {
	flag.Parse()

	config, err := tools.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}

	pserver := printserver.Server{
		Source:      "/amq/queue/printer",
		Destination: "/amq/queue/finished_printing",
		StompConfig: &config.Messaging,
	}

	srv := server{
		Gsprint: "gsprint.exe",
		Workdir: config.Workdirs.Printer,
	}

	for {
		pserver.Process(srv.processIncoming)
		time.Sleep(15 * time.Second)
	}
}
