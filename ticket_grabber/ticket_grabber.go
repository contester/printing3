package main

import (
	"database/sql"
	"flag"
	"fmt"

	"code.google.com/p/goconf/conf"
	"code.google.com/p/log4go"

	_ "github.com/go-sql-driver/mysql"
	"github.com/contester/printing3/tools"
	"time"
	"encoding/json"
)

func createDb(spec string) (db *sql.DB, err error) {
	if db, err = sql.Open("mysql", spec + "?parseTime=true"); err != nil {
		return
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return
}

func createSelectSubmitQuery(extraWhere string) string {
	return `select
Submits.Contest, Submits.Team, Submits.Touched, Submits.Task, Submits.Compiled,
(unix_timestamp(Submits.Arrived) - unix_timestamp(Contests.Start)) as Arrived,
Submits.Passed, Submits.Taken, Submits.ID, Areas.Printer, Contests.SchoolMode, Submits.TestingID,
Schools.Name, Teams.Num, Contests.Name as ContestName,
Problems.Name as ProblemName, inet_ntoa(Submits.Computer) as ComputerID, CompLocations.Name as ComputerName,
Areas.ID as AreaID, Areas.Name as AreaName
from Submits, Contests, Areas, CompLocations, Teams, Schools, Participants, Problems
where
Contests.ID = Submits.Contest and Submits.Finished and ` + extraWhere + `
Submits.Computer = CompLocations.ID and CompLocations.Location = Areas.ID and Participants.LocalID = Submits.Team
and Teams.ID = Participants.Team and Participants.Contest = Submits.Contest and Teams.School = Schools.ID and
Problems.Contest = Submits.Contest and Problems.ID = Submits.Task and Contests.PrintTickets = 1 and Areas.Printer is not NULL
order by Submits.Touched asc
`
}

var (
	allSubmitsQuery = createSelectSubmitQuery("((Submits.Printed is null) or (Submits.Printed < Submits.Touched)) and")
	relatedSubmitsQuery = createSelectSubmitQuery("Submits.Contest = ? and Submits.Task = ? and Submits.Team = ?")
)

type scannedSubmit struct {
	Contest, Team int64
	Touched time.Time
	Task string
	Compiled, Arrived, Passed, Taken, ID int64
	Printer string
	SchoolMode, TestingID int64
	SchoolName string
	TeamNum sql.NullInt64
	ContestName, ProblemName, ComputerID, ComputerName string
	AreaID int64
	AreaName string
}

type rowOrRows interface {
	Scan(dest ...interface{}) error
}

func scanSubmit(r rowOrRows) (*scannedSubmit, error) {
	var sub scannedSubmit
	err := r.Scan(&sub.Contest, &sub.Team, &sub.Touched, &sub.Task, &sub.Compiled, &sub.Arrived, &sub.Passed,
		&sub.Taken, &sub.ID, &sub.Printer, &sub.SchoolMode, &sub.TestingID, &sub.SchoolName, &sub.TeamNum,
		&sub.ContestName, &sub.ProblemName, &sub.ComputerID, &sub.ComputerName, &sub.AreaID, &sub.AreaName)
	if err != nil {
		log4go.Error("Error when scanning: %s", err)
		return nil, err
	}
	return &sub, nil
}

func processSubmit(sub *scannedSubmit) {
	if b, err := json.MarshalIndent(sub, "", "  "); err == nil {
		log4go.Info("%s", string(b))
	}
}

func scan(db *sql.DB) {
	rows, err := db.Query(allSubmitsQuery)
	if err != nil {
		fmt.Printf("Error in submit: %s", err)
		log4go.Error("Error in submit: %s", err)
		return
	}
	for rows.Next() {
		sub, _ := scanSubmit(rows)
		fmt.Println(sub)
		if sub != nil {

			processSubmit(sub)
		}
	}
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	dbSpec := flag.String("db", "", "")
	//stompSpec := flag.String("messaging", "", "")

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

	scan(db)
}
