package main

import (
	"encoding/binary"
	"testing"
)

// fakeExe is the least that clearHighEntropyVA looks at.
func fakeExe(magic, chars uint16) []byte {
	b := make([]byte, 0x200)
	b[0], b[1] = 'M', 'Z'
	binary.LittleEndian.PutUint32(b[0x3c:], 0x80)
	copy(b[0x80:], "PE\x00\x00")
	binary.LittleEndian.PutUint16(b[0x80+24:], magic)
	binary.LittleEndian.PutUint16(b[0x80+24+dllCharsOffset:], chars)
	return b
}

func TestClearHighEntropyVA(t *testing.T) {
	b := fakeExe(0x20b, 0x8160)
	changed, err := clearHighEntropyVA(b)
	if err != nil || !changed {
		t.Fatalf("changed %v, err %v", changed, err)
	}
	if got := binary.LittleEndian.Uint16(b[0x80+24+dllCharsOffset:]); got != 0x8140 {
		t.Errorf("flags %#x, want 0x8140", got)
	}
	if changed, err := clearHighEntropyVA(b); err != nil || changed {
		t.Errorf("second run: changed %v, err %v", changed, err)
	}
}

func TestClearHighEntropyVARefusesOtherFiles(t *testing.T) {
	for name, b := range map[string][]byte{
		"empty":       nil,
		"not a pe":    make([]byte, 0x200),
		"32 bit":      fakeExe(0x10b, 0x8160),
		"bad pointer": append([]byte("MZ"), make([]byte, 0x100)...),
	} {
		if _, err := clearHighEntropyVA(b); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
