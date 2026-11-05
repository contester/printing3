package main

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	winspool         = windows.NewLazySystemDLL("winspool.drv")
	procOpenPrinter  = winspool.NewProc("OpenPrinterW")
	procClosePrinter = winspool.NewProc("ClosePrinter")
	procGetPrinter   = winspool.NewProc("GetPrinterW")
	procEnumJobs     = winspool.NewProc("EnumJobsW")
	procEnumPrinters = winspool.NewProc("EnumPrintersW")
)

const (
	// Printers this user is connected to on print servers.
	printerEnumConnections = 0x4
	printerAttrNetwork     = 0x10
)

// PRINTER_INFO_4W: all that EnumPrinters returns at level 4, without
// contacting the print servers.
type printerInfo4 struct {
	PrinterName *uint16
	ServerName  *uint16
	Attributes  uint32
}

// PRINTER_INFO_2W
type printerInfo2 struct {
	ServerName         *uint16
	PrinterName        *uint16
	ShareName          *uint16
	PortName           *uint16
	DriverName         *uint16
	Comment            *uint16
	Location           *uint16
	DevMode            uintptr
	SepFile            *uint16
	PrintProcessor     *uint16
	Datatype           *uint16
	Parameters         *uint16
	SecurityDescriptor uintptr
	Attributes         uint32
	Priority           uint32
	DefaultPriority    uint32
	StartTime          uint32
	UntilTime          uint32
	Status             uint32
	Jobs               uint32
	AveragePPM         uint32
}

// JOB_INFO_1W
type jobInfo1 struct {
	JobID        uint32
	PrinterName  *uint16
	MachineName  *uint16
	UserName     *uint16
	Document     *uint16
	Datatype     *uint16
	StatusText   *uint16
	Status       uint32
	Priority     uint32
	Position     uint32
	TotalPages   uint32
	PagesPrinted uint32
	Submitted    windows.Systemtime
}

type winSpooler struct{}

func newSpooler() spooler { return winSpooler{} }

func openPrinter(name string) (windows.Handle, error) {
	n, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	var h windows.Handle
	r, _, e := procOpenPrinter.Call(uintptr(unsafe.Pointer(n)), uintptr(unsafe.Pointer(&h)), 0)
	if r == 0 {
		return 0, fmt.Errorf("OpenPrinter(%q): %w", name, e)
	}
	return h, nil
}

func closePrinter(h windows.Handle) { procClosePrinter.Call(uintptr(h)) }

func (winSpooler) Printer(name string) (printerInfo, error) {
	h, err := openPrinter(name)
	if err != nil {
		return printerInfo{}, err
	}
	defer closePrinter(h)

	var need uint32
	procGetPrinter.Call(uintptr(h), 2, 0, 0, uintptr(unsafe.Pointer(&need)))
	if need == 0 {
		return printerInfo{}, fmt.Errorf("GetPrinter(%q): no data", name)
	}
	buf := make([]byte, need)
	r, _, e := procGetPrinter.Call(uintptr(h), 2, uintptr(unsafe.Pointer(&buf[0])), uintptr(need), uintptr(unsafe.Pointer(&need)))
	if r == 0 {
		return printerInfo{}, fmt.Errorf("GetPrinter(%q): %w", name, e)
	}
	pi := (*printerInfo2)(unsafe.Pointer(&buf[0]))
	return printerInfo{
		Status:     pi.Status,
		Attributes: pi.Attributes,
		Driver:     windows.UTF16PtrToString(pi.DriverName),
	}, nil
}

func (winSpooler) Jobs(name string) ([]jobInfo, error) {
	h, err := openPrinter(name)
	if err != nil {
		return nil, err
	}
	defer closePrinter(h)

	var need, count uint32
	// The first call is expected to fail with ERROR_INSUFFICIENT_BUFFER and
	// tells the size; with an empty queue it succeeds and needs nothing.
	procEnumJobs.Call(uintptr(h), 0, 0xffffffff, 1, 0, 0,
		uintptr(unsafe.Pointer(&need)), uintptr(unsafe.Pointer(&count)))
	if need == 0 {
		return nil, nil
	}
	buf := make([]byte, need)
	r, _, e := procEnumJobs.Call(uintptr(h), 0, 0xffffffff, 1, uintptr(unsafe.Pointer(&buf[0])), uintptr(need),
		uintptr(unsafe.Pointer(&need)), uintptr(unsafe.Pointer(&count)))
	if r == 0 {
		return nil, fmt.Errorf("EnumJobs(%q): %w", name, e)
	}
	raw := unsafe.Slice((*jobInfo1)(unsafe.Pointer(&buf[0])), count)
	res := make([]jobInfo, 0, count)
	for _, j := range raw {
		res = append(res, jobInfo{
			ID:           j.JobID,
			Document:     windows.UTF16PtrToString(j.Document),
			Status:       j.Status,
			TotalPages:   j.TotalPages,
			PagesPrinted: j.PagesPrinted,
		})
	}
	return res, nil
}

func (winSpooler) RemotePrinters() ([]remotePrinter, error) {
	var need, count uint32
	// Expected to fail with ERROR_INSUFFICIENT_BUFFER and tell the size.
	procEnumPrinters.Call(printerEnumConnections, 0, 4, 0, 0,
		uintptr(unsafe.Pointer(&need)), uintptr(unsafe.Pointer(&count)))
	if need == 0 {
		return nil, nil
	}
	buf := make([]byte, need)
	r, _, e := procEnumPrinters.Call(printerEnumConnections, 0, 4, uintptr(unsafe.Pointer(&buf[0])), uintptr(need),
		uintptr(unsafe.Pointer(&need)), uintptr(unsafe.Pointer(&count)))
	if r == 0 {
		return nil, fmt.Errorf("EnumPrinters: %w", e)
	}
	raw := unsafe.Slice((*printerInfo4)(unsafe.Pointer(&buf[0])), count)
	var res []remotePrinter
	for _, p := range raw {
		if p.Attributes&printerAttrNetwork == 0 {
			continue
		}
		if rp, ok := parseUNC(windows.UTF16PtrToString(p.PrinterName)); ok {
			res = append(res, rp)
		}
	}
	return res, nil
}
