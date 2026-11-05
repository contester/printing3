package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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

	pdfOnce sync.Once
	pdf     pdfPrinter
	pdfErr  error
}

const (
	defaultJobTimeout = 300 * time.Second
	pollInterval      = 500 * time.Millisecond
)

// printResult is what print learned about a job that was printed.
type printResult struct {
	JobID        uint32
	PagesPrinted int64
	Detail       string
}

// print sends a job to the printer and follows the resulting spooler job.
// submit does the sending. It returns the spooler job id if it knows it, or 0
// when the job has to be found in the queue by its document name. A non-nil
// error means the job is known not to have been printed.
func (s *server) print(printerName, docName string, submit func() (uint32, error)) (printResult, error) {
	if !s.SkipPreflight {
		pi, err := s.spool.Printer(printerName)
		if err != nil {
			return printResult{}, err
		}
		log.Infof("Printer %q: driver %q, status: %s", printerName, pi.Driver, describeBits(pi.Status, printerBits))
		if problem := printerProblem(pi); problem != "" {
			return printResult{}, fmt.Errorf("not sending job to %q: %s", printerName, problem)
		}
	}

	baseline, err := s.spool.Jobs(printerName)
	if err != nil {
		return printResult{}, err
	}
	timeout := defaultJobTimeout
	if s.JobTimeoutSeconds > 0 {
		timeout = time.Duration(s.JobTimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Until submit returns, we don't know whether it will tell us the job id,
	// so look for the job by name in the meantime.
	var submitted atomic.Bool
	watcher := newJobWatcher(baseline, docName)
	result := make(chan jobOutcome, 1)
	go func() {
		result <- watchJob(ctx, s.spool, printerName, watcher, &submitted, pollInterval)
	}()

	id, err := submit()
	if err != nil {
		cancel()
		<-result
		return printResult{JobID: id}, err
	}
	if id != 0 {
		watcher.know(id, docName)
	}
	submitted.Store(true)

	out := <-result
	if !out.Printed {
		return printResult{JobID: out.JobID}, fmt.Errorf("%s", out.Message)
	}
	log.Infof("Spooler job %d finished, %d pages printed", out.JobID, out.PagesPrinted)
	return printResult{
		JobID:        out.JobID,
		PagesPrinted: int64(out.PagesPrinted),
		Detail:       fmt.Sprintf("spooler job %d finished", out.JobID),
	}, nil
}

func (s *server) justPrint(printerName, sourceFullName string) error {
	cmd := exec.Command(s.Gsprint, "-printer", printerName, sourceFullName)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run()
}

// printPS prints a PostScript file with gsprint. The spooler job is found by
// its document name.
func (s *server) printPS(printerName, sourceName, sourceFullName string) (printResult, error) {
	return s.print(printerName, sourceName, func() (uint32, error) {
		return 0, s.justPrint(printerName, sourceFullName)
	})
}

// printPDF renders a PDF through the printer driver with PDFium. The spooler
// tells us the job id.
func (s *server) printPDF(printerName, sourceName string, pdf []byte) (printResult, error) {
	pp, err := s.pdfPrinter()
	if err != nil {
		return printResult{}, err
	}
	return s.print(printerName, sourceName, func() (uint32, error) {
		id, pages, err := pp.Print(printerName, sourceName, pdf)
		log.Infof("Sent %d pages to %q as spooler job %d", pages, printerName, id)
		return id, err
	})
}

func (s *server) pdfPrinter() (pdfPrinter, error) {
	s.pdfOnce.Do(func() {
		dll := s.Pdfium
		if dll == "" {
			dll = "pdfium.dll"
		}
		s.pdf, s.pdfErr = newPDFPrinter(dll)
	})
	return s.pdf, s.pdfErr
}

func (s *server) processIncoming(ctx context.Context, msg *stomp.Message) error {
	var job tpb.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		log.Errorf("Received malformed job: %s", err)
		return tools.MaybeAck(msg)
	}

	var data []byte
	ext := ".ps"
	if pdf, ok := job.GetPayload().(*tpb.BinaryJob_Pdf); ok {
		data, ext = pdf.Pdf, ".pdf"
	} else {
		data = job.GetPs()
	}

	sourceName := time.Now().Format("2006-01-02T15-04-05") + "-" + job.GetJobId() + ext
	sourceFullName := filepath.Join(s.Workdir, sourceName)
	if err := os.WriteFile(sourceFullName, data, 0o644); err != nil {
		log.Errorf("Error writing file: %s", err)
		return tools.SendAndAck(msg, s.FailureQueue, &tpb.PrintJobReport{
			JobExpandedId:    job.GetJobId(),
			ErrorMessage:     err.Error(),
			TimestampSeconds: time.Now().Unix(),
		})
	}

	log.Infof("Sending job %s (%s) to printer %s", job.GetJobId(), ext, job.GetPrinter())
	var res printResult
	var err error
	switch {
	case *dryRun:
		log.Infof("Would print %q on %q", sourceFullName, job.GetPrinter())
	case ext == ".pdf":
		res, err = s.printPDF(job.GetPrinter(), sourceName, data)
	default:
		res, err = s.printPS(job.GetPrinter(), sourceName, sourceFullName)
	}

	rpb := tpb.PrintJobReport{
		JobExpandedId:    job.GetJobId(),
		TimestampSeconds: time.Now().Unix(),
		NumPages:         job.GetPages(),
		PagesPrinted:     res.PagesPrinted,
		SpoolerJobId:     res.JobID,
		Detail:           res.Detail,
	}

	if err != nil {
		log.Errorf("Error printing: %s", err)
		rpb.ErrorMessage = err.Error()
	}

	return tools.SendAndAck(msg, s.FailureQueue, &rpb)
}

type sconfig struct {
	Workdir, Gsprint string
	// Pdfium is the path to pdfium.dll, used for PDF jobs. A relative path
	// is looked up next to the executable. Default pdfium.dll.
	Pdfium                    string
	StompDSN                  string
	BinaryQueue, FailureQueue string

	// JobTimeoutSeconds is how long to wait for a job to leave the print
	// queue. Default 300.
	JobTimeoutSeconds int
	// SkipPreflight disables the check of the printer status before printing.
	SkipPreflight bool

	// AgentName tells this agent from others in the printer list sent to the
	// web application. Default is the host name.
	AgentName string
	// PrintersQueue is where the list of remote printers goes. Default
	// contester.printers.
	PrintersQueue string
	// PrintersIntervalSeconds is how often that list is sent. Default 300.
	PrintersIntervalSeconds int
}

var (
	configFile = flag.String("config", "config.toml", "Config file")
	dryRun     = flag.Bool("dry_run", false, "dry run")
	listOnly   = flag.Bool("list_printers", false, "print the remote printers this agent would report, and exit")
)

func main() {
	flag.Parse()
	srv := server{spool: newSpooler()}
	if _, err := toml.DecodeFile(*configFile, &srv.sconfig); err != nil {
		log.Fatal(err)
	}

	if *listOnly {
		printers, err := srv.spool.RemotePrinters()
		if err != nil {
			log.Fatal(err)
		}
		for _, p := range printerList(srv.agentName(), time.Now(), printers).Printers {
			fmt.Println(p.GetUnc())
		}
		return
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

	go srv.publishPrinters(ctx, conn)

	select {}
}
