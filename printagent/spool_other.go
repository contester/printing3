//go:build !windows

package main

import "errors"

type noSpooler struct{}

func newSpooler() spooler { return noSpooler{} }

var errNoSpooler = errors.New("print spooler is only available on Windows")

func (noSpooler) Printer(string) (printerInfo, error)      { return printerInfo{}, errNoSpooler }
func (noSpooler) Jobs(string) ([]jobInfo, error)           { return nil, errNoSpooler }
func (noSpooler) RemotePrinters() ([]remotePrinter, error) { return nil, errNoSpooler }
