package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	"github.com/jjeffery/stomp"
	"code.google.com/p/log4go"
	"github.com/jmoiron/sqlx"


	_ "github.com/go-sql-driver/mysql"
	"github.com/contester/printing3/tools"
	"github.com/contester/printing3/tickets"
	"time"
	"code.google.com/p/goprotobuf/proto"
	"strconv"
)

func createDb(spec string) (db *sqlx.DB, err error) {
	if db, err = sqlx.Connect("mysql", spec + "?parseTime=true&charset=utf8mb4,utf8"); err != nil {
		return
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
order by Submits.Arrived asc
`
}

var (
	allSubmitsQuery = createSelectSubmitQuery("((Submits.Printed is null) or (Submits.Printed < Submits.Touched)) and")
	relatedSubmitsQuery = createSelectSubmitQuery("Submits.Contest = ? and Submits.Task = ? and Submits.Team = ? and Submits.ID < ? and")
)

type submitProcessor struct {
	*tools.StompConfig
	db *sqlx.DB
	conn *stomp.Conn
	queueName string
}

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

func scanSubmit(r rowOrRows) (result *scannedSubmit, err error) {
	var sub scannedSubmit
	if err = r.Scan(&sub.Contest, &sub.Team, &sub.Touched, &sub.Task, &sub.Compiled, &sub.Arrived, &sub.Passed,
		&sub.Taken, &sub.ID, &sub.Printer, &sub.SchoolMode, &sub.TestingID, &sub.SchoolName, &sub.TeamNum,
		&sub.ContestName, &sub.ProblemName, &sub.ComputerID, &sub.ComputerName, &sub.AreaID, &sub.AreaName); err == nil {
		result = &sub
	}
	return
}

func findRelatedSubmits(db *sqlx.DB, sub *scannedSubmit) ([]*scannedSubmit, error) {
	rows, err := db.Query(relatedSubmitsQuery, sub.Contest, sub.Task, sub.Team, sub.ID)
	if err != nil {
		log4go.Error("Error scanning for related submits: %s", err)
		return nil, err
	}

	result := make([]*scannedSubmit, 0)

	for rows.Next() {
		s, _ := scanSubmit(rows)
		if s != nil {
			result = append(result, s)
		}
	}

	return result, nil
}

func (s *submitProcessor) createSubmit(sub *scannedSubmit, submitNo int) *tickets.Ticket_Submit {
	var result tickets.Ticket_Submit
	result.SubmitNumber = proto.Uint32(uint32(submitNo))
	if sub.Arrived > 0 {
		result.Arrived = proto.Uint64(uint64(sub.Arrived))
	}
	if result.Compiled = proto.Bool(sub.Compiled == 1); !result.GetCompiled() {
		return &result
	}
	if sub.SchoolMode != 0 {
		result.School = &tickets.Ticket_Submit_School{TestsTaken: proto.Uint32(uint32(sub.Taken)), TestsPassed: proto.Uint32(uint32(sub.Passed)),}
	} else {
		var description string
		var test int64
		err := s.db.QueryRow("select ResultDesc.Description, Results.Test from Results, ResultDesc where " +
					"Results.UID = ? and ResultDesc.ID = Results.Result and not ResultDesc.Success order by Results.Test",
			sub.TestingID).Scan(&description, &test)
		switch {
		case err == sql.ErrNoRows:
			if sub.Passed != 0 && sub.Passed == sub.Taken {
				result.Acm = &tickets.Ticket_Submit_ACM{Result: proto.String("ACCEPTED")}
			}
		case err != nil:
			log.Fatal(err)
			return nil
		default:
			result.Acm = &tickets.Ticket_Submit_ACM{Result: &description, TestId: proto.Uint32(uint32(test)),}
		}
	}
	return &result
}

func (s *submitProcessor) processSubmit(sub *scannedSubmit) {
	related, err := findRelatedSubmits(s.db, sub)
	if err != nil {
		return
	}

	var result tickets.Ticket

	result.SubmitId = proto.Uint32(uint32(sub.ID))
	result.Printer = &sub.Printer
	result.Computer = &tickets.Computer{Id: &sub.ComputerID, Name: &sub.ComputerName,}
	result.Area = &tickets.IdName{Id: proto.Uint32(uint32(sub.AreaID)), Name: &sub.AreaName,}
	result.Contest = &tickets.IdName{Id: proto.Uint32(uint32(sub.Contest)), Name: &sub.ContestName,}
	result.Problem = &tickets.Ticket_Problem{Id: &sub.Task, Name: &sub.ProblemName,}

	teamName := sub.SchoolName
	if sub.TeamNum.Valid && sub.TeamNum.Int64 > 0 {
		teamName = teamName + " #" + strconv.FormatInt(sub.TeamNum.Int64, 10)
	}
	result.Team = &tickets.IdName{Id: proto.Uint32(uint32(sub.Team)), Name: &teamName,}
	result.JudgeTime = proto.Uint64(uint64(sub.Touched.UnixNano() / 1000))

	result.Submit = make([]*tickets.Ticket_Submit, 0)
	result.Submit = append(result.Submit, s.createSubmit(sub, len(related) + 1))
	for count := len(related); count > 0; {
		count -= 1
		result.Submit = append(result.Submit, s.createSubmit(related[count], count))
	}

	body, err := proto.Marshal(&result)
	if err != nil {
		return
	}

	for {
		if s.conn == nil {
			s.conn, err = s.StompConfig.NewConnection()
			if err != nil {
				log.Printf("(retry in 15s) Error connecting to stomp: %s", err)
				time.Sleep(15 * time.Second)
				continue
			}
		}

		err = s.conn.SendWithReceipt("/amq/queue/ticket_pb", "application/octet-stream", body, stomp.NewHeader("delivery-mode", "2"))
		if err == nil {
			break
		}
		s.conn.Disconnect()
		s.conn = nil
	}
	s.db.Exec("Update Submits set Printed = Touched where ID = ?", sub.ID)
	fmt.Printf("Printed submit %d\n", sub.ID)
}

func (s *submitProcessor) scan() {
	rows, err := s.db.Query(allSubmitsQuery)
	if err != nil {
		log4go.Error("Error in submit: %s", err)
		return
	}
	for rows.Next() {
		if sub, err := scanSubmit(rows); sub != nil {
			s.processSubmit(sub)
		} else {
			log4go.Error("Error reading submit info: %s", err)
		}
	}
}

func main() {
	tools.SetupLogWrapper()
	defer log4go.Close()

	configFileName := flag.String("config", "", "")
	dbSpec := flag.String("db", "", "")
	stompSpec := flag.String("messaging", "", "")

	flag.Parse()

	config, err := tools.MaybeReadConfigFile(*configFileName)

	if config != nil {
		if s, err := config.GetString("server", "db"); err == nil {
			log4go.Trace("Imported db spec from config file: %s", s)
			*dbSpec = s
		}
	}

	var srv submitProcessor

	srv.db, err = createDb(*dbSpec)
	if err != nil {
		log4go.Error("Opening db connection to %s: %s", *dbSpec, err)
		return
	}

	srv.StompConfig, err = tools.ParseStompFlagOrConfig(*stompSpec, config, "messaging")
	if err != nil {
		log4go.Error("Opening stomp connection to %s: %s", *stompSpec, err)
		return
	}

	for {
		srv.scan()
		log.Printf("Scan complete, sleeping for 15s")
		time.Sleep(15 * time.Second)
	}
}
