package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"

	log "github.com/sirupsen/logrus"
)

var pagesRe = regexp.MustCompile(`^*.dvi: (\d+) page`)

func (s *server) processTex(ctx context.Context, jobID string, content []byte) ([]byte, int64, error) {
	jobDir := filepath.Join(s.TexDir, jobID)
	if err := os.MkdirAll(jobDir, os.ModePerm); err != nil {
		return nil, 0, err
	}

	sourceName := fmt.Sprintf("%s.tex", jobID)

	if err := ioutil.WriteFile(filepath.Join(jobDir, sourceName), content, os.ModePerm); err != nil {
		return nil, 0, err
	}

	cmd := exec.CommandContext(ctx, "latex", "-interaction=batchmode", sourceName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		log.Infof("first latex run has error %v. Rerunning", err)
	}

	cmd = exec.CommandContext(ctx, "latex", "-interaction=batchmode", sourceName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		log.Infof("seconf latex run has error %v. Ignoring because I'm too lazy", err)
	}

	dviName := fmt.Sprintf("%s.dvi", jobID)

	if _, err := os.Stat(filepath.Join(cmd.Dir, dviName)); err != nil {
		return nil, 0, fmt.Errorf("can't find dvi file: %v", err)
	}

	cmd = exec.CommandContext(ctx, "dviinfox", "-p", dviName)
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
	data, err := ioutil.ReadFile(filepath.Join(jobDir, psName))
	return data, pages, err
}
