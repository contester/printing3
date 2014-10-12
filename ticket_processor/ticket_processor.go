package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"strconv"
	"text/template"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"gopkg.in/stomp.v1"

	_ "code.google.com/p/go-charset/data"
)

const DOCUMENT_TEMPLATE = `\documentclass[12pt,a4paper,oneside]{article}
\\usepackage[utf8]{inputenc}
\\usepackage[english,russian]{babel}
\\usepackage{latexsym}
\pagestyle{empty}
%%\setlength{\voffset}{0cm}
\begin{document}
\begin{center}
{\LARGE\textbf{Submission results}}\newline
\vspace{1cm}
\begin{tabular}{|l|p{11cm}|}
\hline
Contest {{.GetContest.GetId}} & {{.GetContest.GetName}} \\\\
\hline
Submit ID & {{.GetSubmitId}} \\\\
\hline
Judge Time & {{.GetJudgeTime}} \\\\
\hline
Team & {{.GetTeam.GetId}} - {{.GetTeam.GetName}} \\\\
\hline
Location & ({{.GetArea.GetId}}) {{.GetArea.GetName}} \\\\
\hline
Workstation & ({{.GetComputer.GetId}}) {{.GetComputer.GetName}} \\\\
\hline
Problem {{.GetProblem.GetId}} & {{.GetProblem.GetName}} \\\\
\hline
\end{tabular}

\vspace{1cm}

\begin{tabular}{|r|l|l|}
\hline
\# & Time & Result \\\\
\hline
$submitLines$\hline
\end{tabular}
\end{center}
\end{document}`

var documentTemplate = template.Must(template.New("source").Parse(DOCUMENT_TEMPLATE))

type templateData struct {
	*tickets.Ticket
}

type submitLine struct {
	first bool
	*tickets.Ticket_Submit
}

func (s *submitLine) getTimeOffset() string {
	return (time.Duration(s.GetArrived()) * time.Second).String()
}

func (s *submitLine) GetTimeOffset() string {
	return s.ifBold(s.getTimeOffset())
}

func (s *submitLine) getVerdict() string {
	if !s.GetCompiled() {
		return "Compilation error"
	}
	if s.Acm != nil {
		if s.GetAcm().TestId != nil {
			return fmt.Sprintf("%s on test %d", s.GetAcm().GetResult(), s.GetAcm().GetTestId())
		}
		return s.GetAcm().GetResult()
	}
	if s.School != nil {
		if s.GetSchool().GetTestsPassed() == s.GetSchool().GetTestsTaken() {
			return "ACCEPTED"
		}
		return fmt.Sprintf("Not accepted (%d / %d)", s.GetSchool().GetTestsPassed(), s.GetSchool().GetTestsTaken())
	}
	return ""
}

func (s *submitLine) GetVerdict() string {
	return s.ifBold(s.getVerdict())
}

func (s *submitLine) ifBold(x string) string {
	if s.first {
		return "\\textbf{" + x + "}"
	}
	return x
}

func (s *templateData) GetJudgeTime() string {
	return time.Unix(0, int64(s.Ticket.GetJudgeTime())*1000).Format(time.RFC3339)
}

func (s *templateData) GetSubmits() []*submitLine {
	var result []*submitLine
	for index, submit := range s.Ticket.GetSubmit() {
		result = append(result, &submitLine{
			first:         index == 0,
			Ticket_Submit: submit,
		})
	}
	return result
}

func processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
	var job tickets.Ticket
	if err := proto.Unmarshal(msg.Body, &job); err != nil {
		log4go.Error("Received malformed job: %s", err)
		return err
	}

	jobId := "t-" + strconv.FormatUint(uint64(job.GetSubmitId()), 10)

	var buf bytes.Buffer
	if err := documentTemplate.Execute(&buf, &templateData{
		Ticket: &job,
	}); err != nil {
		return err
	}

	cBlob, err := tickets.NewBlob(buf.Bytes())
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
		Source:      "/amq/queue/ticket_pb",
		Destination: "/amq/queue/tex",
	}

	pserver.StompConfig, err = tools.ParseStompFlagOrConfig("", config, "messaging")
	if err != nil {
		log.Fatal(err)
	}

	for {
		pserver.Process(processIncoming)
		time.Sleep(15 * time.Second)
	}
}
