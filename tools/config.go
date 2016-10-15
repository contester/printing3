package tools

import (
	"flag"
	"regexp"

	"gopkg.in/gcfg.v1"
	"gopkg.in/stomp.v2"
)

type StompConfig struct {
	Network, Address, Vhost, Username, Password string
	Params                                      map[string]string
}

type StompConnector interface {
	NewConnection() (*stomp.Conn, error)
}

func (s *StompConfig) NewConnection() (*stomp.Conn, error) {
	var opts []func(*stomp.Conn)error
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
/*
func ParseStompDSN(dsn string) (config *StompConfig, err error) {
	if dsn == "" {
		return nil, fmt.Errorf("Empty stomp spec!")
	}

	var config = &StompConfig{}

	matches := dsnPattern.FindStringSubmatch(dsn)
	if matches == nil {
		return nil, fmt.Errorf("Can't match dsn: %s", dsn)
	}
	names := dsnPattern.SubexpNames()

	for i, match := range matches {
		switch names[i] {
		case "user":
			config.Options.Login = match
		case "passwd":
			config.Options.Passcode = match
		case "net":
			config.Network = match
		case "addr":
			config.Address = match
		case "vhost":
			config.Options.Host = match
		case "params":
			for _, v := range strings.Split(match, "&") {
				param := strings.SplitN(v, "=", 2)
				if len(param) != 2 {
					continue
				}
				config.Options.NonStandard.Add(param[0], param[1])
			}
		}
	}

	// Set default network if empty
	if config.Network == "" {
		config.Network = "tcp"
	}

	// Set default address if empty
	if config.Address == "" {
		config.Address = "127.0.0.1:61613"
	}

	return
}
*/

type GlobalConfig struct {
	Server struct {
		Db string
	}
	Messaging StompConfig
	Workdirs  struct {
		TexProcessor, SourceProcessor string
	}
}

/*
func ParseStompFlagOrConfig(flagValue string, config *conf.ConfigFile, section string) (result *StompConfig, err error) {
	if result, err = ParseStompDSN(flagValue); err == nil {
		return
	}

	if config != nil {
		result, err = ParseStompConfig(config, section)
	}

	return
}
*/
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
