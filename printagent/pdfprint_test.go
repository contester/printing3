package main

import "testing"

func TestFitRect(t *testing.T) {
	// A4 in points, at 600 dpi.
	const w, h = 595.0, 842.0
	// Fits entirely.
	x, y, rw, rh := fitRect(100, 100, 72, 72, 200, 300)
	if rw != 100 || rh != 100 || x != 50 || y != 100 {
		t.Errorf("small page: %d %d %d %d", x, y, rw, rh)
	}
	// Too tall: shrunk, aspect ratio kept, and inside the area.
	x, y, rw, rh = fitRect(w, h, 600, 600, 4800, 6900)
	if rw > 4800 || rh > 6900 || x < 0 || y < 0 {
		t.Errorf("outside the area: %d %d %d %d", x, y, rw, rh)
	}
	if rh != 6900 && rw != 4800 {
		t.Errorf("should touch one edge: %d %d", rw, rh)
	}
	if r1, r2 := float64(rw)/float64(rh), w/h; r1/r2 > 1.001 || r1/r2 < 0.999 {
		t.Errorf("aspect ratio changed: %f vs %f", r1, r2)
	}
}

func TestRasterDPI(t *testing.T) {
	if got := rasterDPI(300, 600); got != 300 {
		t.Errorf("coarser than the printer: %d", got)
	}
	if got := rasterDPI(600, 300); got != 300 {
		t.Errorf("finer than the printer: %d", got)
	}
}

func TestBitmapSize(t *testing.T) {
	w, h := bitmapSize(595, 842, 300) // A4
	if w != 2479 || h != 3508 {
		t.Errorf("A4 at 300 dpi: %dx%d", w, h)
	}
	// A page of 200 x 200 inches would be 3.6e9 pixels at 300 dpi.
	w, h = bitmapSize(14400, 14400, 300)
	if w*h > maxRasterPixels || w != h || w < 1000 {
		t.Errorf("huge page: %dx%d", w, h)
	}
	w, h = bitmapSize(0.1, 0.1, 300)
	if w < 1 || h < 1 {
		t.Errorf("tiny page: %dx%d", w, h)
	}
}
