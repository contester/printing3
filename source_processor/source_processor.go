package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"text/template"
	"time"
	"strings"

	// "code.google.com/p/go-charset/charset"
	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"gopkg.in/stomp.v1"

	_ "code.google.com/p/go-charset/data"
	"github.com/contester/printing3/printserver"
)

type server struct {
	Workdir   string
	languages map[string]string
}

const DOCUMENT_TEMPLATE = `\documentclass[12pt,a4paper,oneside]{article}
\usepackage[utf8]{inputenc}
\usepackage[english,russian]{babel}
\usepackage{fancyhdr}
\usepackage{fancyvrb}
\usepackage{lastpage}
\usepackage{latexsym}
\usepackage{amsmath}
\usepackage{color}
\usepackage{alltt}
\usepackage{bold-extra}
\usepackage{marvosym}
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
Team & ({{.GetTeam.GetId}}) {{.GetTeam.GetName}} \\
\hline
Computer & ({{.GetComputer.GetId}}) {{.GetComputer.GetName}} \\
\hline
Location & ({{.GetArea.GetId}}) {{.GetArea.GetName}} \\
\hline
File name & {{.GetFilename}} \\
\hline
Contest & ({{.GetContest.GetId}}) {{.GetContest.GetName}} \\
\hline
Pages & \pageref{LastPage} \\
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

func texEscape(s string) string {
	s = strings.Replace(s, "%", "\\%", -1)
	s = strings.Replace(s, "$", "\\$", -1)
	s = strings.Replace(s, "_", "\\_", -1)
	s = strings.Replace(s, "{", "\\{", -1)
	s = strings.Replace(s, "#", "\\#", -1)
	return s
}

func (s *server) processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
	var job tickets.PrintJob
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		log4go.Error("Received malformed job: %s", err)
		return err
	}

	jobId := "s-" + strconv.FormatUint(uint64(job.GetJobId()), 10)
	job.Team.Name = proto.String(texEscape(job.Team.GetName()))

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

	/*args := []string{"--out-format=latex",
		"--syntax=" + sourceLang,
		"--style=print",
		"--input=" + sourceName,
		"--output=" + outputName,
		"--fragment",
		"--replace-quotes",
		"--replace-tabs=4",
		"--wrap",
		"--wrap-no-numbers",
		"--encoding=" + sourceCharset,
		"--style-outfile=" + styleName}
	if sourceLang == "txt" {
		args = append(args, "--line-numbers")
	}
*/
	args := []string{"-l", "text", "-f", "latex", "-O", "linenos=1,tabsize=4", "-o", outputName, sourceName}
	cmd := exec.Command("pygmentize", args...)
        cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err = cmd.Run(); err != nil {
		return err
	}

	cmd = exec.Command("pygmentize", "-f", "latex", "-S", "bw")
	out, err := cmd.Output()
	if err != nil {
		return err
	}

	if err = ioutil.WriteFile(filepath.Join(jobDir, styleName), out, os.ModePerm); err != nil {
		return err
	}

	job.Filename = proto.String(texEscape(job.GetFilename()))
	data := templateData{
		PrintJob: &job,
	}

	var contentSource io.Reader
	if contentFile, err := os.Open(filepath.Join(jobDir, outputName)); err == nil {
		contentSource = contentFile
		defer contentFile.Close()
	} else {
		return err
	}

	contents, err := ioutil.ReadAll(contentSource)
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

	cBlob, err := tickets.NewBlob(output.Bytes())
	if err != nil {
		return err
	}

	result := tickets.BinaryJob{
		JobId:   &jobId,
		Printer: job.Printer,
		Data:    cBlob,
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
		Source:      "/amq/queue/source_pb",
		Destination: "/amq/queue/tex_processor",
	}

	pserver.StompConfig, err = tools.ParseStompFlagOrConfig("", config, "messaging")
	if err != nil {
		log.Fatal(err)
	}

	languageNames, err := config.GetOptions("languages")
	if err != nil {
		log.Fatal(err)
	}

	sserver := server{
		languages: make(map[string]string),
	}

	if sserver.Workdir, err = config.GetString("workdirs", "source_processor"); err != nil {
		log.Fatal(err)
	}

	os.MkdirAll(sserver.Workdir, os.ModePerm)

	for _, lang := range languageNames {
		v, err := config.GetString("languages", lang)
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range strings.Split(v, " ") {
			sserver.languages[v] = lang
		}
	}

	for {
		pserver.Process(sserver.processIncoming)
		time.Sleep(15 * time.Second)
	}
}
