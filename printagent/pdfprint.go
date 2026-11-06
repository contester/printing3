package main

import "math"

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

// maxRasterPixels bounds the bitmap of one page, about 256 MB at 4 bytes a pixel,
// so that a PDF with a huge page can't exhaust the memory.
const maxRasterPixels = 64 << 20

// rasterDPI is the resolution to render at: the wanted one, but not finer than
// the printer, since the extra pixels would only be thrown away.
func rasterDPI(wanted, printerDPI int) int {
	if wanted > printerDPI {
		return printerDPI
	}
	return wanted
}

// bitmapSize is the size in pixels of a page of pageW x pageH points at dpi,
// reduced to keep the proportions if the bitmap would be too big.
func bitmapSize(pageW, pageH float64, dpi int) (w, h int) {
	pw := pageW / 72 * float64(dpi)
	ph := pageH / 72 * float64(dpi)
	if px := pw * ph; px > maxRasterPixels {
		k := math.Sqrt(maxRasterPixels / px)
		pw, ph = pw*k, ph*k
	}
	return max(int(pw), 1), max(int(ph), 1)
}

// ditherTo1bpp turns a 32 bit BGRx bitmap into a 1 bit one, white = 1, rows padded
// to 4 bytes, by error diffusion (Floyd-Steinberg). Pure black and white stay
// as they are, so text that was rendered without smoothing is not disturbed;
// grays and images become dot patterns.
func ditherTo1bpp(src []byte, w, h int) []byte {
	rowBytes := (w + 31) / 32 * 4
	dst := make([]byte, rowBytes*h)
	cur := make([]int, w+2) // error carried into the current row, with a guard on both sides
	next := make([]int, w+2)
	for y := 0; y < h; y++ {
		row := src[y*w*4 : (y+1)*w*4]
		for x := 0; x < w; x++ {
			b, g, r := int(row[x*4]), int(row[x*4+1]), int(row[x*4+2])
			v := (306*r+601*g+117*b)>>10 + cur[x+1]
			out := 0
			if v >= 128 {
				out = 255
				dst[y*rowBytes+x/8] |= 0x80 >> (x % 8)
			}
			e := v - out
			cur[x+2] += e * 7 / 16
			next[x] += e * 3 / 16
			next[x+1] += e * 5 / 16
			next[x+2] += e * 1 / 16
		}
		cur, next = next, cur
		for i := range next {
			next[i] = 0
		}
	}
	return dst
}
