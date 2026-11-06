//go:build !windows

package main

import "errors"

func newPDFPrinter(dll string, rasterDPI int) (pdfPrinter, error) {
	return nil, errors.New("pdf printing is only available on Windows")
}
