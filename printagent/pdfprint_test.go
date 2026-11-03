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
