package tools

import "github.com/jjeffery/stomp"

type MessagingConfig struct {
	Address string
	Login string
	Passcode string
	Host string
}

func (s *MessagingConfig) NewConnection() (*stomp.Conn, error) {
	var opts stomp.Options
	opts.AcceptVersion = "1.1,1.2"
	if s.Login != "" {
		opts.Login, opts.Passcode = s.Login, s.Passcode
	}
	if s.Host != "" {
		opts.Host = s.Host
	}
	return stomp.Dial("tcp", s.Address, opts)
}
