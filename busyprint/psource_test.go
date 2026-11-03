package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/encoding/charmap"

	tpb "github.com/contester/printing3/tickets"
)

func TestTexEscape(t *testing.T) {
	for in, want := range map[string]string{
		"plain.cpp":       "plain.cpp",
		`a}\input{x}`:     `a\}\textbackslash{}input\{x\}`,
		"a_b%c$d#e&f":     `a\_b\%c\$d\#e\&f`,
		"x~y^z":           `x\textasciitilde{}y\textasciicircum{}z`,
		"line\nbreak\r\t": "line break  ",
	} {
		if got := texEscape(in); got != want {
			t.Errorf("texEscape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEscapedJobDoesNotMutate(t *testing.T) {
	job := &tpb.PrintJob{Filename: "a_b", Team: &tpb.IdName{Name: "x_y"}}
	j := escapedJob(job)
	if job.Filename != "a_b" || job.Team.Name != "x_y" {
		t.Errorf("original job modified: %v", job)
	}
	if j.Filename != `a\_b` || j.Team.Name != `x\_y` {
		t.Errorf("escaped job wrong: %v", j)
	}
	escapedJob(&tpb.PrintJob{}) // nil sub-messages must not panic
}

// End to end: needs pygmentize, latex, dviinfox and dvips.
func TestPipelineHostileFilename(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	secret := t.TempDir() + "/secret.txt"
	os.WriteFile(secret, []byte("TOPSECRETCONTENT"), 0o644)
	s := &server{languageMap: map[string]string{"cpp": "cpp"}}
	s.SourceDir, s.TexDir = t.TempDir(), t.TempDir()
	job := &tpb.PrintJob{
		JobId:    "s-1",
		Filename: `x}\input{` + secret + `}\immediate\write18{touch ` + s.TexDir + `/pwned}.cpp`,
		Team:     &tpb.IdName{Id: 1, Name: `Team #1 \input{` + secret + `}`},
		Data:     []byte("int main() { return 0; }\n"),
	}
	tex, err := s.processSource(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	ps, pages, err := s.processTex(context.Background(), "s-1", tex)
	if err != nil {
		log, _ := os.ReadFile(s.TexDir + "/s-1/s-1.log")
		t.Fatalf("%v\n%s", err, log)
	}
	if pages != 1 {
		t.Errorf("pages = %d", pages)
	}
	if _, err := os.Stat(s.TexDir + "/pwned"); err == nil {
		t.Error("shell escape ran")
	}
	if strings.Contains(string(ps), "TOPSECRET") {
		t.Error("file inclusion worked")
	}
}

// Cyrillic sources in each charset the webapp may announce must go through
// the whole pipeline without latex errors.
func TestPipelineCharsets(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	src := "// Привет, мир\nint main() { return 0; }\n"
	cp1251, err := charmap.Windows1251.NewEncoder().String(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		charset string
		data    string
	}{
		{"windows-1251", cp1251},
		{"", cp1251}, // default
		{"utf-8", src},
		{"utf-8-sig", "\xef\xbb\xbf" + src},
	} {
		t.Run(tc.charset, func(t *testing.T) {
			s := &server{languageMap: map[string]string{"cpp": "cpp"}}
			s.SourceDir, s.TexDir = t.TempDir(), t.TempDir()
			job := &tpb.PrintJob{JobId: "s-2", Filename: "a.cpp", Charset: tc.charset, Data: []byte(tc.data)}
			tex, err := s.processSource(context.Background(), job)
			if err != nil {
				t.Fatal(err)
			}
			if _, pages, err := s.processTex(context.Background(), "s-2", tex); err != nil || pages != 1 {
				t.Fatalf("pages=%d err=%v", pages, err)
			}
			lg, _ := os.ReadFile(s.TexDir + "/s-2/s-2.log")
			if strings.Contains(string(lg), "\n! ") {
				t.Errorf("latex errors:\n%s", lg)
			}
		})
	}
}

// The pdflatex route: same hostile and Cyrillic inputs, a real PDF, A4 pages.
func TestPipelinePDF(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	src := "// Привет, мир\nint main() { return 0; }\n"
	cp1251, err := charmap.Windows1251.NewEncoder().String(src)
	if err != nil {
		t.Fatal(err)
	}
	secret := t.TempDir() + "/secret.txt"
	os.WriteFile(secret, []byte("TOPSECRETCONTENT"), 0o644)
	for _, tc := range []struct {
		name, charset, filename, data string
	}{
		{"cp1251", "windows-1251", "a.cpp", cp1251},
		{"utf8", "utf-8", "a.cpp", src},
		{"hostile", "utf-8", `x}\input{` + secret + `}.cpp`, src},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &server{languageMap: map[string]string{"cpp": "cpp"}}
			s.SourceDir, s.TexDir = t.TempDir(), t.TempDir()
			job := &tpb.PrintJob{JobId: "s-3", Filename: tc.filename, Charset: tc.charset, Data: []byte(tc.data)}
			tex, err := s.processSource(context.Background(), job)
			if err != nil {
				t.Fatal(err)
			}
			pdf, pages, err := s.processTexPDF(context.Background(), "s-3", tex)
			if err != nil || pages != 1 {
				lg, _ := os.ReadFile(s.TexDir + "/s-3/s-3.log")
				t.Fatalf("pages=%d err=%v\n%s", pages, err, lg)
			}
			if !strings.HasPrefix(string(pdf), "%PDF-") {
				t.Fatal("not a pdf")
			}
			// The MediaBox sits in a compressed object stream, so ask pdfinfo.
			if _, err := exec.LookPath("pdfinfo"); err == nil {
				f := filepath.Join(t.TempDir(), "x.pdf")
				os.WriteFile(f, pdf, 0o644)
				out, _ := exec.Command("pdfinfo", f).CombinedOutput()
				if !strings.Contains(string(out), "(A4)") {
					t.Errorf("page is not A4:\n%s", out)
				}
			}
			lg, _ := os.ReadFile(s.TexDir + "/s-3/s-3.log")
			if strings.Contains(string(lg), "\n! ") {
				t.Errorf("latex errors:\n%s", lg)
			}
			if strings.Contains(string(pdf), "TOPSECRET") {
				t.Error("file inclusion worked")
			}
		})
	}
}
