package main

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/go-stomp/stomp"
	"github.com/kelseyhightower/envconfig"

	tpb "github.com/contester/printing3/tickets"
	log "github.com/sirupsen/logrus"
)

type server struct {
	SourceWorkDirectory string
	TexWorkDirectory    string

	languages map[string]string
}

func (s *server) processPrintJob(ctx context.Context, job *tpb.PrintJob) error {
	texSource, err := s.processSource(ctx, *job)
	if err != nil {
		return err
	}

	jobID := "s-" + strconv.FormatUint(uint64(job.GetJobId()), 10)

	dviBinary, err := s.processTex(ctx, jobID, texSource)
	if err != nil {
		return err
	}

}

type bconfig struct {
	Stomp string

	SourceWorkDirectory string `envconfig:"SOURCE_DIR"`
	TexWorkDirectory    string `envconfig:"TEX_DIR"`

	Languages []string `envconfig:"LANGUAGES"`
}

var dsnPattern = regexp.MustCompile(
	`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
		`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`\/(?P<vhost>.*?)` + // /dbname
		`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]

type stompConfig struct {
	network, address string
	opts             []func(*stomp.Conn) error
}

func (s stompConfig) Dial() (*stomp.Conn, error) {
	return stomp.Dial(s.network, s.address, s.opts...)
}

func parseStompDSN(s string) (stompConfig, error) {
	var result stompConfig
	m := dsnPattern.FindStringSubmatch(s)
	if len(m) == 0 {
		return result, fmt.Errorf("can't parse dsn %q", s)
	}

	cg := make(map[string]string)
	cgn := dsnPattern.SubexpNames()
	for i, v := range m {
		cg[cgn[i]] = v
	}

	result.address = cg["addr"]
	result.network = cg["net"]
	username := cg["user"]
	password := cg["passwd"]
	if username != "" || password != "" {
		result.opts = append(result.opts, stomp.ConnOpt.Login(username, password))
	}
	if vhost := cg["vhost"]; vhost != "" {
		result.opts = append(result.opts, stomp.ConnOpt.Host(vhost))
	}
	return result, nil
}

func main() {
	var bconf bconfig
	if err := envconfig.Process("busyprint", &bconf); err != nil {
		log.Fatal(err)
	}
}
