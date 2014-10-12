package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"gopkg.in/stomp.v1"
)

type server struct {
	Workdir, Gsprint string
}

func (s *server) processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
	var job tickets.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		log.Printf("Received malformed job: %s", err)
		return err
	}

	sourceName := fmt.Sprintf("%s-%s.ps", time.Now().Format(time.RFC3339), job.GetJobId())
	sourceFullName := filepath.Join(s.Workdir, sourceName)
	if buf, err := job.Data.Bytes(); err == nil {
		if err = ioutil.WriteFile(sourceFullName, buf, os.ModePerm); err != nil {
			log.Printf("Error writing file: %s", err)
			return err
		}
	} else {
		log.Printf("Error getting buffer: %s", err)
		return err
	}

	log.Printf("Sending job %s to printer %s", job.GetJobId(), job.GetPrinter())
	cmd := exec.Command(s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	if err := cmd.Run(); err != nil {
		log.Printf("Error printing: %s", err)
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
		Source: "/amq/queue/printer",
	}

	pserver.StompConfig, err = tools.ParseStompFlagOrConfig("", config, "messaging")
	if err != nil {
		log.Fatal(err)
	}

	var srv server
	srv.Gsprint = "gsprint.exe"
	srv.Workdir = os.TempDir()

	for {
		pserver.Process(srv.processIncoming)
		time.Sleep(15 * time.Second)
	}
}
