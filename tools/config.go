package tools

import (
	"github.com/jjeffery/stomp"
	"regexp"
	"strings"
	"code.google.com/p/goconf/conf"
)

type StompConfig struct {
	Network, Address string
	Options stomp.Options
}

func (s *StompConfig) NewConnection() (*stomp.Conn, error) {
	return stomp.Dial(s.Network, s.Address, s.Options)
}

var dsnPattern = regexp.MustCompile(
			`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
					`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
				`\/(?P<vhost>.*?)` + // /dbname
			`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]

func ParseStompDSN(dsn string) (config *StompConfig, err error) {
	if dsn == "" {
		return
	}

	config = &StompConfig{}

	matches := dsnPattern.FindStringSubmatch(dsn)
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

	// Set default adress if empty
	if config.Address == "" {
		config.Address = "127.0.0.1:61613"
	}

	return
}

func ParseStompConfig(config *conf.ConfigFile, section string) (result *StompConfig, err error) {
	var cf StompConfig
	cf.Network = "tcp"
	if cf.Address, err = config.GetString("messaging", "address"); err != nil {
		return
	}

	if cf.Options.Login, err = config.GetString("messaging", "username"); err != nil {
		return
	}

	if cf.Options.Passcode, err = config.GetString("messaging", "password"); err != nil {
		return
	}

	if cf.Options.Host, err = config.GetString("messaging", "vhost"); err != nil {
		return
	}

	return &cf, nil
}
