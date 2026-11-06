package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"

	log "github.com/sirupsen/logrus"
)

var (
	gdi32            = windows.NewLazySystemDLL("gdi32.dll")
	procCreateDC     = gdi32.NewProc("CreateDCW")
	procDeleteDC     = gdi32.NewProc("DeleteDC")
	procStartDoc     = gdi32.NewProc("StartDocW")
	procEndDoc       = gdi32.NewProc("EndDoc")
	procAbortDoc     = gdi32.NewProc("AbortDoc")
	procStartPage    = gdi32.NewProc("StartPage")
	procEndPage      = gdi32.NewProc("EndPage")
	procGetDeviceCap = gdi32.NewProc("GetDeviceCaps")

	procStretchDIBits     = gdi32.NewProc("StretchDIBits")
	procSetStretchBltMode = gdi32.NewProc("SetStretchBltMode")
)

const (
	devTechnology = 2
	devHorzRes    = 8
	devVertRes    = 10
	devBitsPixel  = 12
	devPlanes     = 14
	devNumColors  = 24
	devLogPixelX  = 88
	devLogPixelY  = 90

	fpdfPrinting = 0x800
	// Without these PDFium smooths the edges of text and paths with gray, which a 1 bit image has to dither.
	fpdfNoSmoothText = 0x1000
	fpdfNoSmoothPath = 0x4000

	stretchColorOnColor = 3 // COLORONCOLOR: pixels are repeated or dropped, not blended
	dibRGBColors        = 0
	srcCopy             = 0x00CC0020
)

// BITMAPINFOHEADER
type bitmapInfoHeader struct {
	Size          uint32
	Width, Height int32
	Planes, Bits  uint16
	Compression   uint32
	SizeImage     uint32
	XPelsPerMeter int32
	YPelsPerMeter int32
	ClrUsed       uint32
	ClrImportant  uint32
}

// DOCINFOW
type docInfo struct {
	Size     int32
	DocName  *uint16
	Output   *uint16
	Datatype *uint16
	Type     uint32
}

// FS_SIZEF
type sizeF struct{ Width, Height float32 }

type pdfium struct {
	mu sync.Mutex // PDFium is not thread safe

	dll                *windows.LazyDLL
	loadMemDocument    *windows.LazyProc
	closeDocument      *windows.LazyProc
	getPageCount       *windows.LazyProc
	getPageSizeByIndex *windows.LazyProc
	loadPage           *windows.LazyProc
	closePage          *windows.LazyProc
	renderPage         *windows.LazyProc
	getLastError       *windows.LazyProc

	createBitmap     *windows.LazyProc
	fillBitmap       *windows.LazyProc
	bitmapBuffer     *windows.LazyProc
	bitmapStride     *windows.LazyProc
	destroyBitmap    *windows.LazyProc
	renderPageBitmap *windows.LazyProc

	// rasterDPI > 0: pages are rendered into a bitmap and sent as one image.
	rasterDPI int
	// rasterMono: send a 1 bit image even if the printer says it has more colors. Without it, 1 bit images
	// are sent to printers that say they have just 1 bit.
	rasterMono bool
}

// BITMAPINFO with two colors, for a 1 bit image.
type bitmapInfo1 struct {
	Header bitmapInfoHeader
	Colors [2][4]byte // black, white; blue, green, red, unused
}

// newPDFPrinter loads pdfium.dll. A relative path is looked up next to the
// executable, since a service doesn't start in its own directory.
func newPDFPrinter(dll string, rasterDPI int, rasterMono bool) (pdfPrinter, error) {
	if !filepath.IsAbs(dll) {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		dll = filepath.Join(filepath.Dir(exe), dll)
	}
	p := &pdfium{dll: windows.NewLazyDLL(dll), rasterDPI: rasterDPI, rasterMono: rasterMono}
	if err := p.dll.Load(); err != nil {
		return nil, fmt.Errorf("can't load %s: %w", dll, err)
	}
	p.loadMemDocument = p.dll.NewProc("FPDF_LoadMemDocument")
	p.closeDocument = p.dll.NewProc("FPDF_CloseDocument")
	p.getPageCount = p.dll.NewProc("FPDF_GetPageCount")
	p.getPageSizeByIndex = p.dll.NewProc("FPDF_GetPageSizeByIndexF")
	p.loadPage = p.dll.NewProc("FPDF_LoadPage")
	p.closePage = p.dll.NewProc("FPDF_ClosePage")
	p.renderPage = p.dll.NewProc("FPDF_RenderPage")
	p.getLastError = p.dll.NewProc("FPDF_GetLastError")
	p.createBitmap = p.dll.NewProc("FPDFBitmap_Create")
	p.fillBitmap = p.dll.NewProc("FPDFBitmap_FillRect")
	p.bitmapBuffer = p.dll.NewProc("FPDFBitmap_GetBuffer")
	p.bitmapStride = p.dll.NewProc("FPDFBitmap_GetStride")
	p.destroyBitmap = p.dll.NewProc("FPDFBitmap_Destroy")
	p.renderPageBitmap = p.dll.NewProc("FPDF_RenderPageBitmap")
	p.dll.NewProc("FPDF_InitLibrary").Call()
	return p, nil
}

func (p *pdfium) Print(printer, docName string, pdf []byte) (uint32, int, error) {
	if len(pdf) == 0 {
		return 0, 0, fmt.Errorf("empty pdf")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	doc, _, _ := p.loadMemDocument.Call(uintptr(unsafe.Pointer(&pdf[0])), uintptr(len(pdf)), 0)
	if doc == 0 {
		e, _, _ := p.getLastError.Call()
		return 0, 0, fmt.Errorf("pdfium can't load the document, error %d", e)
	}
	defer p.closeDocument.Call(doc)
	n, _, _ := p.getPageCount.Call(doc)
	pages := int(int32(n))
	if pages <= 0 {
		return 0, 0, fmt.Errorf("pdf has no pages")
	}

	printerName, err := windows.UTF16PtrFromString(printer)
	if err != nil {
		return 0, 0, err
	}
	spooler, _ := windows.UTF16PtrFromString("WINSPOOL")
	dc, _, e := procCreateDC.Call(uintptr(unsafe.Pointer(spooler)), uintptr(unsafe.Pointer(printerName)), 0, 0)
	if dc == 0 {
		return 0, 0, fmt.Errorf("CreateDC(%q): %w", printer, e)
	}
	defer procDeleteDC.Call(dc)

	name, err := windows.UTF16PtrFromString(docName)
	if err != nil {
		return 0, 0, err
	}
	di := docInfo{Size: int32(unsafe.Sizeof(docInfo{})), DocName: name}
	id, _, e := procStartDoc.Call(dc, uintptr(unsafe.Pointer(&di)))
	if int32(id) <= 0 {
		return 0, 0, fmt.Errorf("StartDoc(%q): %w", printer, e)
	}
	jobID := uint32(id)

	if err := p.renderPages(dc, doc, pages); err != nil {
		procAbortDoc.Call(dc)
		return jobID, 0, err
	}
	if r, _, e := procEndDoc.Call(dc); int32(r) <= 0 {
		return jobID, 0, fmt.Errorf("EndDoc: %w", e)
	}
	return jobID, pages, nil
}

func (p *pdfium) renderPages(dc, doc uintptr, pages int) error {
	caps := func(i uintptr) int { r, _, _ := procGetDeviceCap.Call(dc, i); return int(int32(r)) }
	areaW, areaH := caps(devHorzRes), caps(devVertRes)
	dpiX, dpiY := caps(devLogPixelX), caps(devLogPixelY)
	if areaW <= 0 || areaH <= 0 || dpiX <= 0 || dpiY <= 0 {
		return fmt.Errorf("printer reports no usable page area (%dx%d at %dx%d dpi)", areaW, areaH, dpiX, dpiY)
	}

	depth := caps(devBitsPixel) * caps(devPlanes)
	mono := depth == 1 || p.rasterMono
	log.Infof("Printer device: page area %dx%d px at %dx%d dpi, %d bit(s) per pixel, technology %d, %d colors; %s",
		areaW, areaH, dpiX, dpiY, depth, caps(devTechnology), caps(devNumColors), p.describeMode(mono))

	for i := 0; i < pages; i++ {
		var sz sizeF
		if r, _, _ := p.getPageSizeByIndex.Call(doc, uintptr(i), uintptr(unsafe.Pointer(&sz))); r == 0 {
			return fmt.Errorf("can't get size of page %d", i+1)
		}
		page, _, _ := p.loadPage.Call(doc, uintptr(i))
		if page == 0 {
			return fmt.Errorf("can't load page %d", i+1)
		}
		x, y, w, h := fitRect(float64(sz.Width), float64(sz.Height), dpiX, dpiY, areaW, areaH)

		if r, _, e := procStartPage.Call(dc); int32(r) <= 0 {
			p.closePage.Call(page)
			return fmt.Errorf("StartPage %d: %w", i+1, e)
		}
		if p.rasterDPI > 0 {
			if err := p.renderRaster(dc, page, sz, min(dpiX, dpiY), mono, x, y, w, h); err != nil {
				p.closePage.Call(page)
				return fmt.Errorf("page %d: %w", i+1, err)
			}
		} else {
			p.renderPage.Call(dc, page, uintptr(x), uintptr(y), uintptr(w), uintptr(h), 0, fpdfPrinting)
		}
		p.closePage.Call(page)
		if r, _, e := procEndPage.Call(dc); int32(r) <= 0 {
			return fmt.Errorf("EndPage %d: %w", i+1, e)
		}
	}
	return nil
}

// renderRaster renders the page into a bitmap and sends that to the printer as
// one image, into the rectangle x, y, w, h. Some drivers crash on the drawing
// calls PDFium makes (text, clips, images) but cope with a plain image.
func (p *pdfium) renderRaster(dc, page uintptr, sz sizeF, printerDPI int, mono bool, x, y, w, h int) error {
	bw, bh := bitmapSize(float64(sz.Width), float64(sz.Height), rasterDPI(p.rasterDPI, printerDPI))
	// No alpha channel: 4 bytes a pixel, blue first, the last one unused.
	bmp, _, _ := p.createBitmap.Call(uintptr(bw), uintptr(bh), 0)
	if bmp == 0 {
		return fmt.Errorf("can't make a %dx%d bitmap", bw, bh)
	}
	defer p.destroyBitmap.Call(bmp)
	p.fillBitmap.Call(bmp, 0, 0, uintptr(bw), uintptr(bh), 0xFFFFFFFF)
	flags := uintptr(fpdfPrinting)
	if mono {
		flags |= fpdfNoSmoothText | fpdfNoSmoothPath
	}
	p.renderPageBitmap.Call(bmp, page, 0, 0, uintptr(bw), uintptr(bh), 0, flags)

	buf, _, _ := p.bitmapBuffer.Call(bmp)
	stride, _, _ := p.bitmapStride.Call(bmp)
	if buf == 0 || int(int32(stride)) != bw*4 {
		return fmt.Errorf("unexpected bitmap layout: stride %d for width %d", int32(stride), bw)
	}
	if mono {
		return sendMono(dc, unsafe.Slice(*(**byte)(unsafe.Pointer(&buf)), bw*bh*4), bw, bh, x, y, w, h)
	}
	// A negative height makes the image top-down, as PDFium wrote it.
	bi := bitmapInfoHeader{Size: uint32(unsafe.Sizeof(bitmapInfoHeader{})), Width: int32(bw), Height: -int32(bh), Planes: 1, Bits: 32}
	procSetStretchBltMode.Call(dc, stretchColorOnColor)
	r, _, e := procStretchDIBits.Call(dc, uintptr(x), uintptr(y), uintptr(w), uintptr(h),
		0, 0, uintptr(bw), uintptr(bh), buf, uintptr(unsafe.Pointer(&bi)), dibRGBColors, srcCopy)
	runtime.KeepAlive(&bi)
	if int32(r) <= 0 {
		return fmt.Errorf("StretchDIBits: %w", e)
	}
	return nil
}

func (p *pdfium) describeMode(mono bool) string {
	switch {
	case p.rasterDPI <= 0:
		return "PDFium draws on the printer"
	case mono:
		return fmt.Sprintf("pages are sent as 1 bit images, rendered at %d dpi at most", p.rasterDPI)
	}
	return fmt.Sprintf("pages are sent as color images, rendered at %d dpi at most", p.rasterDPI)
}

// sendMono sends a BGRx bitmap to the printer as a 1 bit image that is dithered here, so that the
// driver doesn't convert colors to its own halftones.
func sendMono(dc uintptr, bgrx []byte, bw, bh, x, y, w, h int) error {
	bits := ditherTo1bpp(bgrx, bw, bh)
	bi := bitmapInfo1{
		Header: bitmapInfoHeader{Size: uint32(unsafe.Sizeof(bitmapInfoHeader{})), Width: int32(bw), Height: -int32(bh),
			Planes: 1, Bits: 1, ClrUsed: 2, ClrImportant: 2},
		Colors: [2][4]byte{{0, 0, 0, 0}, {255, 255, 255, 0}},
	}
	procSetStretchBltMode.Call(dc, stretchColorOnColor)
	r, _, e := procStretchDIBits.Call(dc, uintptr(x), uintptr(y), uintptr(w), uintptr(h),
		0, 0, uintptr(bw), uintptr(bh), uintptr(unsafe.Pointer(&bits[0])), uintptr(unsafe.Pointer(&bi)), dibRGBColors, srcCopy)
	runtime.KeepAlive(bits)
	runtime.KeepAlive(&bi)
	if int32(r) <= 0 {
		return fmt.Errorf("StretchDIBits (1 bit): %w", e)
	}
	return nil
}
