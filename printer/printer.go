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

	"github.com/golang/protobuf/proto"
	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"gopkg.in/stomp.v2"
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

	sourceName := fmt.Sprintf("%s-%s.ps", time.Now().Format("2006-01-02T15-04-05"), job.GetJobId())
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
	if tools.DryRun() {
		log.Printf("Would run: %s\n", s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
		return nil
	}
	cmd := exec.Command(s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
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
		Destination: "/amq/queue/finished_printing",
		StompConfig: &config.Messaging,
	}

	var srv server
	srv.Gsprint = "gsprint.exe"
	srv.Workdir, err = config.GetString("workdirs", "printer")
	if err != nil {
		log.Fatal(err)
	}

	for {
		pserver.Process(srv.processIncoming)
		time.Sleep(15 * time.Second)
	}
}
