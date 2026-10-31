package main

import (
	"context"
	"os"
	"strings"
	"testing"

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
