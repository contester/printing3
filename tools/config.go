package tools

import (
	"flag"
	"regexp"

	"gopkg.in/gcfg.v1"
	"github.com/go-stomp/stomp"
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
