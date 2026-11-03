package main

// pdfPrinter renders a PDF onto a printer through its Windows driver.
type pdfPrinter interface {
	// Print sends all pages of the PDF to the printer as one document named
	// docName. It returns the spooler job id and the number of pages sent.
	Print(printer, docName string, pdf []byte) (jobID uint32, pages int, err error)
}

// fitRect places a page of pageW x pageH points on a printable area of
// areaW x areaH device pixels at the given resolution. The page is printed
// at 100% if it fits, otherwise shrunk to fit, and centered either way.
func fitRect(pageW, pageH float64, dpiX, dpiY, areaW, areaH int) (x, y, w, h int) {
	pw := pageW / 72 * float64(dpiX)
	ph := pageH / 72 * float64(dpiY)
	scale := 1.0
	if sx := float64(areaW) / pw; sx < scale {
		scale = sx
	}
	if sy := float64(areaH) / ph; sy < scale {
		scale = sy
	}
	w, h = int(pw*scale), int(ph*scale)
	return (areaW - w) / 2, (areaH - h) / 2, w, h
}
