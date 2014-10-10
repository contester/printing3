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
	Workdir, Queue, Destination string
}

func (s *server) processIncoming(conn *stomp.Conn, msg *stomp.Message) error {
	var job tickets.BinaryJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
                log4go.Error("Received malformed job: %s", err)
		return err
	}

	jobDir := filepath.Join(s.Workdir, job.GetJobId())
	os.MkdirAll(jobDir, os.ModePerm) // err?

	sourceName := fmt.Sprintf("%s.tex", job.GetJobId())

	buf, err := job.GetData().Bytes()
	if err != nil {
		return err
	}
	if err = ioutil.WriteFile(filepath.Join(jobDir, sourceName), buf, os.ModePerm); err != nil {
		return err
	}

	cmd := exec.Command("latex", "-interaction=batchmode", sourceName)
	cmd.Dir = jobDir
	if err = cmd.Run(); err != nil {
		return err
	}

	cmd = exec.Command("latex", "-interaction=batchmode", sourceName)
	cmd.Dir = jobDir
	if err = cmd.Run(); err != nil {
		return err
	}

	dviName := fmt.Sprintf("%s.dvi", job.GetJobId())
	cmd = exec.Command("dvips", "-t", "a4", dviName)
	cmd.Dir = jobDir
	if err = cmd.Run(); err != nil {
		return err
	}

	content, err := ioutil.ReadFile(filepath.Join(jobDir, dviName))
	if err != nil {
		return err
	}

	cBlob, err := tickets.NewBlob(content)
	if err != nil {
		return err
	}

	result := tickets.BinaryJob{
		JobId: job.JobId,
		Printer: job.Printer,
		Data: cBlob,
	}

	content, err = proto.Marshal(&result)
	if err != nil {
		return err
	}

	return conn.Send(s.Destination, "application/binary", content, nil)
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	stompSpec := flag.String("messaging", "", "")

	flag.Parse()

	var srv server
    srv.Queue = "/amq/queue/tex"
	srv.Destination = "/amq/queue/printer"

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
