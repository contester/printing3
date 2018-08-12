package printserver

import (
	log "github.com/sirupsen/logrus"

	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"github.com/golang/protobuf/proto"
)

type Server struct {
	StompConfig         *tools.StompConfig
	Source, Destination string
}

type ServerConn struct {
	server *Server
	conn   tools.Conn
}

func (s *ServerConn) Send(msg proto.Message) error {
	log.Debugf("send: %s", msg)
	contents, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.SendContents(contents, "")
}

func (s *ServerConn) SendContents(contents []byte, contentType string) error {
	if s.server.Destination == "" {
		return nil
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return s.conn.Send(s.server.Destination, contentType, contents,
		stomp.SendOpt.Header("delivery-mode", "2"))
}

func (s *Server) Process(process func(*ServerConn, *stomp.Message) error) error {
	conn, err := s.StompConfig.NewConnection()
	//defer conn.Disconnect()

	if err != nil {
		return err
	}

	sub, err := conn.Subscribe(s.Source, stomp.AckClient)
	if err != nil {
		return err
	}

	for {
		msg, err := sub.Read()
		if err != nil {
			return err
		}
		if s.Destination != "" {
			tx := conn.Begin()
			log.Debugf("Started transaction: %+v", tx)
			err = process(&ServerConn{server: s, conn: tx}, msg)
			if err != nil {
				log.Errorf("Processing error %v, aborting", err)
				tx.Abort()
			} else {
				log.Debugf("Acking")
				if err = tx.Ack(msg); err != nil {
					log.Errorf("Ack error: %v", err)
					conn.Disconnect()
					return err
				}
				log.Debugf("Committing")
				err = tx.Commit()
			}
			log.Debugf("Finished.")
		} else {
			err = process(&ServerConn{server: s, conn: conn}, msg)
			if err != nil {
				err = conn.Nack(msg)
			} else {
				err = conn.Ack(msg)
			}
		}
		if err != nil {
			return err
		}
	}
}
