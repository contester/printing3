// Command lowva clears the HIGH_ENTROPY_VA flag in the header of a 64-bit Windows executable, in place.
//
// printagent runs the printer driver inside its own process. The driver of the HP LaserJet P1102
// (HP1100GC.dll) cuts 64-bit pointers to 32 bits and crashes when its buffers are above 4 GB. A process
// without the flag keeps its images and heaps below 4 GB, which hides the bug. Go marks every executable
// with the flag and has no option to leave it out.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

const (
	highEntropyVA  = 0x20
	dllCharsOffset = 70 // in the optional header of a PE32+ file
)

// clearHighEntropyVA clears the flag in the image b and reports whether it was set.
func clearHighEntropyVA(b []byte) (bool, error) {
	if len(b) < 0x40 || b[0] != 'M' || b[1] != 'Z' {
		return false, errors.New("not a Windows executable (no MZ header)")
	}
	pe := int(binary.LittleEndian.Uint32(b[0x3c:]))
	opt := pe + 24 // signature (4) and file header (20)
	if pe < 0 || opt+dllCharsOffset+2 > len(b) || string(b[pe:pe+4]) != "PE\x00\x00" {
		return false, errors.New("not a Windows executable (no PE header)")
	}
	if magic := binary.LittleEndian.Uint16(b[opt:]); magic != 0x20b {
		return false, fmt.Errorf("not a 64-bit executable (optional header magic %#x)", magic)
	}
	chars := binary.LittleEndian.Uint16(b[opt+dllCharsOffset:])
	if chars&highEntropyVA == 0 {
		return false, nil
	}
	binary.LittleEndian.PutUint16(b[opt+dllCharsOffset:], chars&^highEntropyVA)
	return true, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: lowva file.exe")
		os.Exit(2)
	}
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	changed, err := clearHighEntropyVA(b)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	if changed {
		if err := os.WriteFile(os.Args[1], b, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Printf("%s: HIGH_ENTROPY_VA cleared (was set: %v)\n", os.Args[1], changed)
}
