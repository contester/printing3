package main

import (
	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"flag"
	"fmt"
	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"
		"gopkg.in/stomp.v1"

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
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err = cmd.Run(); err != nil {
		log.Printf("Latex error: %s\n", err)
		return nil
	}

	cmd = exec.Command("latex", "-interaction=batchmode", sourceName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err = cmd.Run(); err != nil {
		log.Printf("Latex error: %s\n", err)
		return nil
	}

	dviName := fmt.Sprintf("%s.dvi", job.GetJobId())
	cmd = exec.Command("dvips", "-t", "a4", dviName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err = cmd.Run(); err != nil {
		log.Printf("Dvips error: %s\n", err)
		return nil
	}

	psName := fmt.Sprintf("%s.ps", job.GetJobId())
	content, err := ioutil.ReadFile(filepath.Join(jobDir, psName))
	if err != nil {
		log.Printf("Where's my file? %s\n", err)
		return nil
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
		Source:      "/amq/queue/tex_processor",
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
