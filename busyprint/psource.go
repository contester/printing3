package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"unicode"

	"google.golang.org/protobuf/proto"

	tpb "github.com/contester/printing3/tickets"
)

const documentTemplateString = `\documentclass[12pt,a4paper,oneside]{article}
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

var documentTemplate = template.Must(template.New("source").Parse(documentTemplateString))

type templateData struct {
	*tpb.PrintJob
	StyleText, IncludeText string
	Encoding               string
}

var texReplacer = strings.NewReplacer(
	`\`, `\textbackslash{}`,
	`{`, `\{`,
	`}`, `\}`,
	`#`, `\#`,
	`$`, `\$`,
	`%`, `\%`,
	`&`, `\&`,
	`_`, `\_`,
	`~`, `\textasciitilde{}`,
	`^`, `\textasciicircum{}`,
)

// texEscape makes s safe to put into running text of a TeX document.
// Control characters (including newlines) are replaced by spaces.
func texEscape(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	return texReplacer.Replace(s)
}

// escapedJob returns a copy of job with every string that ends up in the
// document template escaped. The original job is not modified.
func escapedJob(job *tpb.PrintJob) *tpb.PrintJob {
	j := proto.Clone(job).(*tpb.PrintJob)
	j.Filename = texEscape(j.GetFilename())
	if j.Team != nil {
		j.Team.Name = texEscape(j.Team.Name)
	}
	if j.Contest != nil {
		j.Contest.Name = texEscape(j.Contest.Name)
	}
	if j.Area != nil {
		j.Area.Name = texEscape(j.Area.Name)
	}
	if j.Computer != nil {
		j.Computer.Id = texEscape(j.Computer.Id)
		j.Computer.Name = texEscape(j.Computer.Name)
	}
	return j
}

func (s *server) processSource(ctx context.Context, job *tpb.PrintJob) ([]byte, error) {
	jobID := job.GetJobId()

	jobDir := filepath.Join(s.SourceDir, jobID)
	if err := os.MkdirAll(jobDir, os.ModePerm); err != nil {
		return nil, err
	}

	sourceLang := filepath.Ext(job.GetFilename())
	if sourceLang != "" {
		sourceLang = s.languageMap[sourceLang[1:]]
	}
	if sourceLang == "" {
		sourceLang = "txt"
	}

	sourceName := fmt.Sprintf("%s-source.%s", jobID, sourceLang)
	outputName := fmt.Sprintf("%s-hl.tex", jobID)

	if err := os.WriteFile(filepath.Join(jobDir, sourceName), job.GetData(), os.ModePerm); err != nil {
		return nil, err
	}

	sourceCharset := job.GetCharset()
	if sourceCharset == "" {
		sourceCharset = "cp1251"
	}

	args := []string{"-l", "text", "-f", "latex", "-O", "linenos=1,tabsize=4,outencoding=utf8,encoding=" + sourceCharset, "-o", outputName, sourceName}
	cmd := exec.Command("pygmentize", args...)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.Command("pygmentize", "-f", "latex", "-S", "bw")
	styleBytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	data := templateData{
		PrintJob: escapedJob(job),
	}

	bs, err := os.ReadFile(filepath.Join(jobDir, outputName))
	if err != nil {
		return nil, err
	}
	data.IncludeText = string(bs)
	data.StyleText = string(styleBytes)

	var output bytes.Buffer
	if err = documentTemplate.Execute(&output, &data); err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}
