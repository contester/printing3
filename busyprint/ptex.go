package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

func (s *server) processTex(ctx context.Context, jobID string, content []byte) ([]byte, error) {
	jobDir := filepath.Join(s.TexDir, jobID)
	if err := os.MkdirAll(jobDir, os.ModePerm); err != nil {
		return nil, err
	}

	sourceName := fmt.Sprintf("%s.tex", jobID)

	if err := ioutil.WriteFile(filepath.Join(jobDir, sourceName), content, os.ModePerm); err != nil {
		return nil, err
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

	log.Infof("dvips: %s", jobID)
	dviName := fmt.Sprintf("%s.dvi", jobID)
	cmd = exec.CommandContext(ctx, "dvips", "-t", "a4", dviName)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = jobDir, os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	psName := fmt.Sprintf("%s.ps", jobID)
	return ioutil.ReadFile(filepath.Join(jobDir, psName))
}
