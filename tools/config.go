package tools

import (
	"context"
	"flag"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/go-stomp/stomp"
	"gopkg.in/gcfg.v1"
)

type StompConfig struct {
	Network, Address, Vhost, Username, Password string
	Params                                      map[string]string
}

type StompConnector interface {
	NewConnection() (*stomp.Conn, error)
}

func (s *StompConfig) NewConnection() (*stomp.Conn, error) {
	var opts []func(*stomp.Conn) error
	network := s.Network
	if network == "" {
		network = "tcp"
	}
	if s.Username != "" {
		opts = append(opts, stomp.ConnOpt.Login(s.Username, s.Password))
	}
	if s.Vhost != "" {
		opts = append(opts, stomp.ConnOpt.Host(s.Vhost))
	}
	return stomp.Dial(network, s.Address, opts...)
}

var dsnPattern = regexp.MustCompile(
	`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
		`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`\/(?P<vhost>.*?)` + // /dbname
		`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]

type GlobalConfig struct {
	Server struct {
		Db string
	}
	Messaging StompConfig
	Workdirs  struct {
		TexProcessor    string `gcfg:"tex-processor"`
		SourceProcessor string `gcfg:"source-processor"`
		Printer         string `gcfg:"printer"`
	}
	Languages map[string]*struct{ Ext string }
}

var (
	configFileName = flag.String("config", "", "Config file path")
	dryRun         = flag.Bool("dry_run", false, "Dry run")
)

func DryRun() bool {
	return *dryRun
}

func ReadConfig() (*GlobalConfig, error) {
	var gc GlobalConfig
	return &gc, gcfg.ReadFileInto(&gc, *configFileName)
}

func ParseStompDSN(s string) (StompConfig, error) {
	var result StompConfig
	m := dsnPattern.FindStringSubmatch(s)
	if len(m) == 0 {
		return result, fmt.Errorf("can't parse dsn %q", s)
	}

	cgn := dsnPattern.SubexpNames()
	for i, v := range m {
		switch cgn[i] {
		case "addr":
			result.Address = v
		case "net":
			result.Network = v
		case "user":
			result.Username = v
		case "passwd":
			result.Username = v
		case "vhost":
			result.Vhost = v
		}
	}

	if result.Network == "" {
		result.Network = "tcp"
	}

	if result.Address == "" {
		result.Address = "localhost"
	}
	if strings.IndexByte(result.Address, ":") == -1 {
		result.Address += ":61613"
	}

	return result, nil
}

func DialStomp(ctx context.Context, cfg StompConfig) (*stomp.Conn, error) {
	var d net.Dialer

	conn, err := d.DialContext(ctx, cfg.Network, cfg.Address)
	if err != nil {
		return nil, err
	}

	var opts []func(*stomp.Conn) error
	if cfg.Username != "" {
		opts = append(opts, stomp.ConnOpt.Login(cfg.Username, cfg.Password))
	}
	if cfg.Vhost != "" {
		opts = append(opts, stomp.ConnOpt.Host(cfg.Vhost))
	}

	sc, err := stomp.Connect(conn, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return sc, nil
}
