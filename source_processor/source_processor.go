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
	"strconv"
	"code.google.com/p/go-charset/charset"
	_ "code.google.com/p/go-charset/data"
	"code.google.com/p/goconf/conf"
	"github.com/contester/runlib/contester_proto"
	"text/template"
)

type server struct {
	*tools.StompConfig
	Workdir, Queue string
	languages map[string]string
}

const DOCUMENT_TEMPLATE = `\documentclass[12pt,a4paper,oneside]{article}
\u005cusepackage[utf8]{inputenc}
\u005cusepackage[english,russian]{babel}
\u005cusepackage{fancyhdr}
\u005cusepackage{lastpage}
\u005cusepackage{latexsym}
\u005cusepackage{color}
\u005cusepackage{alltt}
\u005cusepackage{bold-extra}
\renewcommand{\familydefault}{\ttdefault}
\pagestyle{fancy}
\lhead{({{.GetComputer.GetId}}) {{.GetComputer.GetName}}}
\chead{}
\rhead{({{.GetTeam.GetId}}) {{.GetTeam.GetName}}}
\lfoot{({{.GetArea.GetId}}) {{.GetArea.GetName}}}
\cfoot{ {{.GetFilename}}}
\rfoot{\thepage\ of \pageref{LastPage}}
{{.StyleText}}
\hoffset=-20mm
\voffset=-20mm
\setlength\textheight{245mm}
\setlength\textwidth{175mm}
\fancyhfoffset{0cm}
\title{ {{.GetFilename}}}
\begin{document}

\begin{center}
\begin{tabular}{|l|p{11cm}|}
\hline
Team & ({{.GetTeam.GetId}}) {{.GetTeam.GetName}} \\\\
\hline
Computer & ({{.GetComputer.GetId}}) {{.GetComputer.GetName}} \\\\
\hline
Location & ({{.GetArea.GetId}}) {{.GetArea.GetName}} \\\\
\hline
File name & {{.GetFilename}} \\\\
\hline
Contest & ({{.GetContest.GetId}}) {{.GetContest.GetName}} \\\\
\hline
Pages & \pageref{LastPage} \\\\
\hline
\end{tabular}
\end{center}
\thispagestyle{empty}
{{.IncludeText}}
\end {document}`

var documentTemplate = template.Must(template.New("source").Parse(DOCUMENT_TEMPLATE))

type templateData struct {
	*tickets.PrintJob
	StyleText, IncludeText string
}

func (s *server) processIncoming(conn *stomp.Conn, msg *stomp.Message) error {
	var job tickets.PrintJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
                log4go.Error("Received malformed job: %s", err)
		return err
	}

	jobId := strconv.FormatUint(uint64(job.GetJobId()), 10)

	jobDir := filepath.Join(s.Workdir, jobId)
	os.MkdirAll(jobDir, os.ModePerm) // err?

	sourceLang := filepath.Ext(job.GetFilename())
	if sourceLang != "" {
		sourceLang = s.languages[sourceLang[1:]]
	}
	if sourceLang == "" {
		sourceLang = "txt"
	}

	sourceName := fmt.Sprintf("%s-source.%s", jobId, sourceLang)
	outputName := fmt.Sprintf("%s-hl.tex", jobId)
	styleName := fmt.Sprintf("%s-style.sty", jobId)

	buf, err := job.GetData().Bytes()
	if err != nil {
		return err
	}

	if err = ioutil.WriteFile(filepath.Join(jobDir, sourceName), buf, os.ModePerm); err != nil {
		return err
	}

	sourceCharset := job.GetCharset()
	if sourceCharset == "" {
		sourceCharset = "cp1251"
	}

	args := []string{"--out-format=latex",
		"--syntax=" + sourceLang,
		"--style=print",
		"--input=" + sourceName,
		"--output=" + outputName,
		"--fragment",
		"--replace-quotes",
		"--wrap",
		"--encoding=" + sourceCharset,
		"--style-outfile=" + styleName}
	if sourceLang == "txt" {
		args = append(args, "--line-numbers")
	}

	cmd := exec.Command("highlight", args...)
	if err = cmd.Run(); err != nil {
		return err
	}

	data := templateData{
		PrintJob: &job,
	}

	contentFile, err := os.Open(filepath.Join(jobDir, outputName))
	if err != nil {
		return err
	}
	defer contentFile.Close()

	if sourceCharset != "utf-8" {
		contentFile, err = charset.NewReader(sourceCharset, contentFile)
		if err != nil {
			return nil
		}
	}

	contents, err := ioutil.ReadAll(contentFile)
	if err != nil {
		return err
	}

	data.IncludeText = string(contents)
	contents, err = ioutil.ReadFile(filepath.Join(jobDir, styleName))
	if err == nil && len(contents) != 0 {
		data.StyleText = string(contents)
	}

	var output bytes.Buffer

	if err = documentTemplate.Execute(&output, &data); err != nil {
		return err
	}

	msg := tickets.BinaryJob{
		JobId: &jobId,
		Printer: job.Printer,
		Data: tickets.NewBlob(output.Bytes()),
	}

	contents, err = proto.Marshal(&msg)
	if err != nil {
		return err
	}

	return conn.Send(s.Destination, "application/binary", contents, nil)
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	stompSpec := flag.String("messaging", "", "")

	flag.Parse()

	var srv server
    srv.Queue = "/amq/queue/sources"
	srv.Destination = "/amq/queue/tex"

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
