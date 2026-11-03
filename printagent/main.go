package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"google.golang.org/protobuf/proto"

	tpb "github.com/contester/printing3/tickets"
	log "github.com/sirupsen/logrus"
)

type server struct {
	sconfig
	spool spooler
}

const (
	defaultJobTimeout = 300 * time.Second
	pollInterval      = 500 * time.Millisecond
)

// print sends the file to the printer and follows the resulting spooler job.
// It returns the number of pages the spooler reports as printed (0 if it
// couldn't tell) and an error if the job is known not to have been printed.
func (s *server) print(printerName, sourceFullName string) (int64, error) {
	if !s.SkipPreflight {
		pi, err := s.spool.Printer(printerName)
		if err != nil {
			return 0, err
		}
		log.Infof("Printer %q: driver %q, status: %s", printerName, pi.Driver, describeBits(pi.Status, printerBits))
		if problem := printerProblem(pi); problem != "" {
			return 0, fmt.Errorf("not sending job to %q: %s", printerName, problem)
		}
	}

	baseline, err := s.spool.Jobs(printerName)
	if err != nil {
		return 0, err
	}
	timeout := defaultJobTimeout
	if s.JobTimeoutSeconds > 0 {
		timeout = time.Duration(s.JobTimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var submitted atomic.Bool
	watcher := newJobWatcher(baseline, filepath.Base(sourceFullName))
	result := make(chan jobOutcome, 1)
	go func() {
		result <- watchJob(ctx, s.spool, printerName, watcher, &submitted, pollInterval)
	}()

	if err := s.justPrint(printerName, sourceFullName); err != nil {
		cancel()
		<-result
		return 0, err
	}
	submitted.Store(true)

	out := <-result
	switch {
	case out.Printed:
		log.Infof("Spooler job %d finished, %d pages printed", out.JobID, out.PagesPrinted)
	}
	if !out.Printed {
		return 0, fmt.Errorf("%s", out.Message)
	}
	return int64(out.PagesPrinted), nil
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
	if err := os.WriteFile(sourceFullName, job.GetData(), os.ModePerm); err != nil {
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
		_, err = s.print(job.GetPrinter(), sourceFullName)
	}

	rpb := tpb.PrintJobReport{
		JobExpandedId:    job.GetJobId(),
		TimestampSeconds: time.Now().Unix(),
		NumPages:         job.GetPages(),
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

	// JobTimeoutSeconds is how long to wait for a job to leave the print
	// queue. Default 300.
	JobTimeoutSeconds int
	// SkipPreflight disables the check of the printer status before printing.
	SkipPreflight bool
}

var (
	configFile = flag.String("config", "config.toml", "Config file")
	dryRun     = flag.Bool("dry_run", false, "dry run")
)

func main() {
	flag.Parse()
	srv := server{spool: newSpooler()}
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
