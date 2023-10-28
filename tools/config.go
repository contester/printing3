package tools

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/go-stomp/stomp"
)

type StompConfig struct {
	Network, Address, Vhost, Username, Password string
	Params                                      map[string]string
}

var dsnPattern = regexp.MustCompile(
	`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
		`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`\/(?P<vhost>.*?)` + // /dbname
		`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]

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
			result.Password = v
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
	if strings.Index(result.Address, ":") == -1 {
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
