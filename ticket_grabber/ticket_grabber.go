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

const SELECT_QUERY = `select
a.Contest, a.Team, a.Touched, a.Task, a.Compiled,
(unix_timestamp(a.Arrived) - unix_timestamp(Contests.Start)) as Arrived,
a.Passed, a.Taken, a.ID, count(b.ID) as SubmitNo, Areas.Printer,
Contests.SchoolMode, a.TestingID, Schools.Name, Teams.Num,
Contests.Name as ContestName, Problems.Name as ProblemName,
inet_ntoa(a.Computer) as ComputerID, CompLocations.Name as ComputerName,
Areas.ID as AreaID, Areas.Name as AreaName
from
Submits as a, Submits as b, Contests, Areas, CompLocations, Teams, Schools, Participants, Problems
Contests.ID = a.Contest, a.Finished, a.Contest = b.Contest, a.Team = b.Team,
a.Task = b.Task, b.Arrived <= a.Arrived, ((a.Printed is null) or (a.Printed < a.Touched)),
a.Computer = CompLocations.ID, CompLocations.Location = Areas.ID, Participants.LocalID = a.Team,
Teams.ID = Participants.Team, Participants.Contest = a.Contest, Teams.School = Schools.ID,
Problems.Contest = a.Contest, Problems.ID = a.Task, Contests.PrintTickets = 1, Areas.Printer is not NULL
group by a.Task, a.Compiled, a.Arrived, a.Passed, a.Taken, a.ID
order by a.Touched asc
`

func scan(db *sql.DB) {
	db.QueryRow()
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
