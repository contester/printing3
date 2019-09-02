package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	tpb "github.com/contester/printing3/tickets"
)

const documentTemplateString = `\documentclass[12pt,a4paper,oneside]{article}
\usepackage[cp1251]{inputenc}
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

var documentTemplate = template.Must(template.New("source").Parse(documentTemplateString))

type templateData struct {
	*tpb.PrintJob
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

func (s *server) processSource(ctx context.Context, job *tpb.PrintJob) ([]byte, error) {
	jobID := job.GetJobId()
	job.Team.Name = texEscape(job.Team.GetName())

	jobDir := filepath.Join(s.SourceWorkDirectory, jobID)
	if err := os.MkdirAll(jobDir, os.ModePerm); err != nil {
		return nil, err
	}

	sourceLang := filepath.Ext(job.GetFilename())
	if sourceLang != "" {
		sourceLang = s.languages[sourceLang[1:]]
	}
	if sourceLang == "" {
		sourceLang = "txt"
	}

	sourceName := fmt.Sprintf("%s-source.%s", jobID, sourceLang)
	outputName := fmt.Sprintf("%s-hl.tex", jobID)
	styleName := fmt.Sprintf("%s-style.sty", jobID)

	if err := ioutil.WriteFile(filepath.Join(jobDir, sourceName), job.GetData(), os.ModePerm); err != nil {
		return nil, err
	}

	sourceCharset := job.GetCharset()
	if sourceCharset == "" {
		sourceCharset = "cp1251"
	}

	args := []string{"-l", "text", "-f", "latex", "-O", "linenos=1,tabsize=4,encoding=" + sourceCharset, "-o", outputName, sourceName}
	cmd := exec.Command("pygmentize", args...)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.Command("pygmentize", "-f", "latex", "-S", "bw", "-o", styleName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	job.Filename = texEscape(job.GetFilename())
	data := templateData{
		PrintJob: job,
	}

	bs, err := ioutil.ReadFile(filepath.Join(jobDir, outputName))
	if err != nil {
		return nil, err
	}
	data.IncludeText = string(bs)
	bs, err = ioutil.ReadFile(filepath.Join(jobDir, styleName))
	if err != nil {
		return nil, err
	}
	data.StyleText = string(bs)

	var output bytes.Buffer
	if err = documentTemplate.Execute(&output, &data); err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}
