package tools

import (
	"github.com/jjeffery/stomp"
	"regexp"
	"strings"
	"code.google.com/p/goconf/conf"
	"fmt"
)

type StompConfig struct {
	Network, Address string
	Options stomp.Options
}

type StompConnector interface {
	NewConnection() (*stomp.Conn, error)
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
		return nil, fmt.Errorf("Empty stomp spec!")
	}

	config = &StompConfig{}

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

	// Set default adress if empty
	if config.Address == "" {
		config.Address = "127.0.0.1:61613"
	}

	return
}

func ParseStompConfig(config *conf.ConfigFile, section string) (result *StompConfig, err error) {
	var cf StompConfig
	cf.Network = "tcp"
	if cf.Address, err = config.GetString(section, "address"); err != nil {
		return
	}

	if cf.Options.Login, err = config.GetString(section, "username"); err != nil {
		return
	}

	if cf.Options.Passcode, err = config.GetString(section, "password"); err != nil {
		return
	}

	if cf.Options.Host, err = config.GetString(section, "vhost"); err != nil {
		return
	}

	return &cf, nil
}

func ParseStompFlagOrConfig(flagValue string, config *conf.ConfigFile, section string) (result *StompConfig, err error) {
	if result, err = ParseStompDSN(flagValue); err == nil {
		return
	}

	if config != nil {
		result, err = ParseStompConfig(config, section)
	}

	return
}
