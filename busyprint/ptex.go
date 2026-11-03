package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"

	log "github.com/sirupsen/logrus"
)

var pagesRe = regexp.MustCompile(`^*.dvi: (\d+) page`)

// pdfPagesRe matches the last line pdflatex writes to the log.
var pdfPagesRe = regexp.MustCompile(`Output written on .*\.pdf \((\d+) pages?`)

// latexCmd builds a invocation of a TeX engine that can't run external commands.
// Reading arbitrary files is NOT prevented here (openin_any=p was tested and
// does not block absolute paths); that relies on texEscape for all inputs.
func latexCmd(ctx context.Context, engine, jobDir, sourceName string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, engine, "-no-shell-escape", "-interaction=batchmode", sourceName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), "openout_any=p", "shell_escape=f")
	return cmd
}

// runTex writes the source and runs the engine twice (the second run resolves
// \pageref{LastPage}). Errors from the engine are only logged: the caller
// checks for the output file, which is what matters.
func runTex(ctx context.Context, engine, texDir, jobID string, content []byte) (string, error) {
	jobDir := filepath.Join(texDir, jobID)
	if err := os.MkdirAll(jobDir, os.ModePerm); err != nil {
		return "", err
	}

	sourceName := fmt.Sprintf("%s.tex", jobID)

	if err := os.WriteFile(filepath.Join(jobDir, sourceName), content, os.ModePerm); err != nil {
		return "", err
	}

	if err := latexCmd(ctx, engine, jobDir, sourceName).Run(); err != nil {
		log.Infof("first %s run has error %v. Rerunning", engine, err)
	}
	if err := latexCmd(ctx, engine, jobDir, sourceName).Run(); err != nil {
		log.Infof("second %s run has error %v. Ignoring because I'm too lazy", engine, err)
	}
	return jobDir, nil
}

// processTexPDF compiles the job straight to PDF with pdflatex.
func (s *server) processTexPDF(ctx context.Context, jobID string, content []byte) ([]byte, int64, error) {
	jobDir, err := runTex(ctx, "pdflatex", s.TexDir, jobID, content)
	if err != nil {
		return nil, 0, err
	}

	logData, err := os.ReadFile(filepath.Join(jobDir, jobID+".log"))
	if err != nil {
		return nil, 0, fmt.Errorf("can't read tex log: %v", err)
	}
	groups := pdfPagesRe.FindSubmatch(logData)
	if len(groups) < 2 {
		return nil, 0, fmt.Errorf("pdflatex produced no output")
	}
	pages, err := strconv.ParseInt(string(groups[1]), 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to parse pages into int: %q %v", string(groups[1]), err)
	}

	data, err := os.ReadFile(filepath.Join(jobDir, jobID+".pdf"))
	return data, pages, err
}

func (s *server) processTex(ctx context.Context, jobID string, content []byte) ([]byte, int64, error) {
	jobDir, err := runTex(ctx, "latex", s.TexDir, jobID, content)
	if err != nil {
		return nil, 0, err
	}

	dviName := fmt.Sprintf("%s.dvi", jobID)

	if _, err := os.Stat(filepath.Join(jobDir, dviName)); err != nil {
		return nil, 0, fmt.Errorf("can't find dvi file: %v", err)
	}

	cmd := exec.CommandContext(ctx, "dviinfox", "-p", dviName)
	cmd.Dir = jobDir

	pagesTxt, err := cmd.CombinedOutput()
	if err != nil {
		return nil, 0, fmt.Errorf("unable to retrieve number of pages")
	}

	groups := pagesRe.FindSubmatch(pagesTxt)
	if len(groups) < 2 {
		return nil, 0, fmt.Errorf("unable to find pages in %q", string(pagesTxt))
	}

	pages, err := strconv.ParseInt(string(groups[1]), 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to parse pages into int: %q %v", string(groups[1]), err)
	}

	cmd = exec.CommandContext(ctx, "dvips", "-t", "a4", dviName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, pages, err
	}

	psName := fmt.Sprintf("%s.ps", jobID)
	data, err := os.ReadFile(filepath.Join(jobDir, psName))
	return data, pages, err
}
