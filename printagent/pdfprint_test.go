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

// bgrx makes a w x h bitmap of one gray level.
func bgrx(w, h int, gray byte) []byte {
	b := make([]byte, w*h*4)
	for i := 0; i < w*h; i++ {
		b[i*4], b[i*4+1], b[i*4+2] = gray, gray, gray
	}
	return b
}

func ones(bits []byte, w, h int) int {
	rowBytes := (w + 31) / 32 * 4
	n := 0
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			if bits[y*rowBytes+x/8]&(0x80>>(x%8)) != 0 {
				n++
			}
		}
	}
	return n
}

func TestDitherTo1bpp(t *testing.T) {
	const w, h = 70, 40 // not a multiple of 8, to see the padding
	if got := len(ditherTo1bpp(bgrx(w, h, 255), w, h)); got != 12*h {
		t.Errorf("size %d, want %d", got, 12*h)
	}
	if n := ones(ditherTo1bpp(bgrx(w, h, 255), w, h), w, h); n != w*h {
		t.Errorf("white: %d of %d pixels set", n, w*h)
	}
	if n := ones(ditherTo1bpp(bgrx(w, h, 0), w, h), w, h); n != 0 {
		t.Errorf("black: %d pixels set", n)
	}
	// Half gray: about half of the pixels.
	if n := ones(ditherTo1bpp(bgrx(w, h, 128), w, h), w, h); n < w*h*45/100 || n > w*h*55/100 {
		t.Errorf("gray 128: %d of %d pixels set", n, w*h)
	}
	// Black text on white keeps its shape: a black column stays black.
	img := bgrx(w, h, 255)
	for y := 0; y < h; y++ {
		img[(y*w+10)*4], img[(y*w+10)*4+1], img[(y*w+10)*4+2] = 0, 0, 0
	}
	bits := ditherTo1bpp(img, w, h)
	rowBytes := (w + 31) / 32 * 4
	for y := 0; y < h; y++ {
		if bits[y*rowBytes+10/8]&(0x80>>(10%8)) != 0 || bits[y*rowBytes+9/8]&(0x80>>(9%8)) == 0 {
			t.Fatalf("row %d: the black column is not a clean edge", y)
		}
	}
}
