package main

import (
	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"flag"
	"fmt"
	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"github.com/jjeffery/stomp"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type server struct {
	Workdir string
}

func (s *server) processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
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
		JobId:   job.JobId,
		Printer: job.Printer,
		Data:    cBlob,
	}

	content, err = proto.Marshal(&result)
	if err != nil {
		return err
	}

	return conn.Send(&result)
}

func main() {
	flag.Parse()

	config, err := tools.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}

	pserver := printserver.Server{
		Source:      "/amq/queue/tex",
		Destination: "/amq/queue/printer",
	}

	pserver.StompConfig, err = tools.ParseStompFlagOrConfig("", config, "messaging")
	if err != nil {
		log.Fatal(err)
	}

	var sserver server
	if sserver.Workdir, err = config.GetString("workdirs", "tex_processor"); err != nil {
		log.Fatal(err)
	}

	os.MkdirAll(sserver.Workdir, os.ModePerm)

	for {
		pserver.Process(sserver.processIncoming)
		time.Sleep(15 * time.Second)
	}
}
