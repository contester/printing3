package main

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"github.com/golang/protobuf/proto"

	tpb "github.com/contester/printing3/tickets"
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

func (s *server) processIncoming(ctx context.Context, msg *stomp.Message) error {
	var job tpb.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		log.Errorf("Received malformed job: %s", err)
		return tools.MaybeAck(msg)
	}

	sourceName := time.Now().Format("2006-01-02T15-04-05") + "-" + job.GetJobId() + ".ps"
	sourceFullName := filepath.Join(s.Workdir, sourceName)
	if err := ioutil.WriteFile(sourceFullName, job.GetData(), os.ModePerm); err != nil {
		log.Errorf("Error writing file: %s", err)
		return tools.SendAndAck(msg, s.FailureQueue, &tpb.PrintJobReport{
			JobExpandedId:    job.GetJobId(),
			ErrorMessage:     err.Error(),
			TimestampSeconds: time.Now().Unix(),
		})
	}

	log.Infof("Sending job %s to printer %s", job.GetJobId(), job.GetPrinter())
	var err error
	if *dryRun {
		log.Infof("Would run: %q %s %q %q", s.Gsprint, "-printer", job.GetPrinter(), sourceFullName)
	} else {
		err = s.justPrint(job.GetPrinter(), sourceFullName)
	}

	rpb := tpb.PrintJobReport{
		JobExpandedId:    job.GetJobId(),
		TimestampSeconds: time.Now().Unix(),
	}

	if err != nil {
		log.Errorf("Error printing: %s", err)
		rpb.ErrorMessage = err.Error()
	}

	return tools.SendAndAck(msg, s.FailureQueue, &rpb)
}

type sconfig struct {
	Workdir, Gsprint          string
	StompDSN                  string
	BinaryQueue, FailureQueue string
}

var (
	configFile = flag.String("config", "config.toml", "Config file")
	dryRun     = flag.Bool("dry_run", false, "dry run")
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

	sub, err := tools.SubscribeAndProcess(ctx, conn, srv.BinaryQueue, srv.processIncoming)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	select {}
}
