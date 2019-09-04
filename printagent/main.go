package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

type server struct {
	sconfig
}

func (s *server) justPrint(printerName, sourceFullName string) error {
	cmd := exec.Command(s.Gsprint, "-printer", printerName, sourceFullName)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run()
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
		log.Errorf("Error getting buffer: %s", err)
		return err
	}

	log.Infof("Sending job %s to printer %s", job.GetJobId(), job.GetPrinter())
	var err error
	if tools.DryRun() {
		log.Infof("Would run: %q %s %q %q", s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	} else {
		err = s.justPrint(job.GetPrinter(), sourceFullName)
	}
	if err != nil {
		log.Errorf("Error printing: %s", err)
		return err
	}

	type printDone struct {
		ID string `json:"id"`
	}

	outbuf, err := json.Marshal(printDone{ID: job.GetJobId()})
	if err != nil {
		return nil
	}
	return conn.SendContents(outbuf, "application/json")
}

type sconfig struct {
	Workdir, Gsprint string
	StompDSN         string
}

var (
	configFile = flag.String("config", "config.toml", "Config file")
)

func main() {
	flag.Parse()
	var srv server
	if _, err := toml.DecodeFile(*configFile, &srv.sconfig); err != nil {
		log.Fatal(err)
	}

	sconf, err := tools.ParseStompDSN(srv.sconfig.StompDSN)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	conn, err := tools.DialStomp(ctx, sconf)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.MustDisconnect()

	


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
