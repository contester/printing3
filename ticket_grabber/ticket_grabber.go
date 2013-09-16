package main

import (
	"database/sql"
	"strings"
	"net"
	"flag"
	"net/http"

	"code.google.com/p/goconf/conf"
	"code.google.com/p/log4go"

	_ "github.com/go-sql-driver/mysql"
)

func createDb(spec string) (db *sql.DB, err error) {
	if db, err = sql.Open("mysql", spec); err != nil {
		return
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	dbSpec := flag.String("db", "", "")
	stompSpec := flag.String("messaging", "", "")

	flag.Parse()

	if *configFileName != "" {
		config, err := conf.ReadConfigFile(*configFileName)
		if err != nil {
			log4go.Error("Reading config file: %s", err)
			return
		}

		if s, err := config.GetString("server", "db"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			*dbSpec = s
		}
	}

	db, err := createDb(*dbSpec)
	if err != nil {
		log4go.Exitf("Opening db connection to %s: %s", *dbSpec, err)
		return
	}
}
