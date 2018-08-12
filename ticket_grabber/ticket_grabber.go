package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"strconv"
	"time"

	"github.com/contester/printing3/printserver"
	"github.com/contester/printing3/tickets"
	"github.com/contester/printing3/tools"
	"github.com/go-stomp/stomp"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"

	log "github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

func createSelectSubmitQuery(extraWhere string) string {
	return `select
Submits.Contest as contest,
Submits.Team as team,
Submits.Touched as touched,
Submits.Task as task,
Submits.Compiled as compiled,
(unix_timestamp(Submits.Arrived) - unix_timestamp(Contests.Start)) as arrived,
Submits.Passed as passed,
Submits.Taken as taken,
Submits.ID as id,
Areas.Printer as printer,
Contests.SchoolMode as schoolmode,
Submits.TestingID as testingid,
Schools.Name as schoolname,
Teams.Num as teamnum,
Contests.Name as contestname,
Problems.Name as problemname,
inet_ntoa(Submits.Computer) as computerid,
CompLocations.Name as computername,
Areas.ID as areaid,
Areas.Name as areaname
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
	allSubmitsQuery     = createSelectSubmitQuery("((Submits.Printed is null) or (Submits.Printed < Submits.Touched)) and")
	relatedSubmitsQuery = createSelectSubmitQuery("Submits.Contest = ? and Submits.Task = ? and Submits.Team = ? and Submits.ID < ? and")
	submitById          = createSelectSubmitQuery("Submits.ID = ? and")
)

type scannedSubmit struct {
	Contest, Team                                      int64
	Touched                                            time.Time
	Task                                               string
	Compiled, Arrived, Passed, Taken, ID               int64
	Printer                                            string
	SchoolMode, TestingID                              int64
	SchoolName                                         string
	TeamNum                                            sql.NullInt64
	ContestName, ProblemName, ComputerID, ComputerName string
	AreaID                                             int64
	AreaName                                           string
}

type submitTicket struct {
	Submit struct {
		Id int32 `json:"id"`
	} `json:"submit"`
}

type hasScanx interface {
	StructScan(interface{}) error
}

func scanSubmit(r hasScanx) (*scannedSubmit, error) {
	log.Debugf("Scanning...")
	var sub scannedSubmit
	if err := r.StructScan(&sub); err != nil {
		return nil, err
	}
	return &sub, nil
}

func findRelatedSubmits(db *sqlx.DB, sub *scannedSubmit) ([]*scannedSubmit, error) {
	rows, err := db.Queryx(relatedSubmitsQuery, sub.Contest, sub.Task, sub.Team, sub.ID)
	if err != nil {
		log.Errorf("Scanning for related submits: %v", err)
		return nil, err
	}

	var result []*scannedSubmit

	for rows.Next() {
		if s, err := scanSubmit(rows); err == nil {
			result = append(result, s)
		}
	}

	return result, nil
}

func createSubmit(db *sqlx.DB, sub *scannedSubmit, submitNo int) *tickets.Ticket_Submit {
	result := tickets.Ticket_Submit{
		SubmitNumber: uint32(submitNo),
		Arrived:      uint64(sub.Arrived),
		Compiled:     sub.Compiled == 1,
	}
	if !result.Compiled {
		return &result
	}
	if sub.SchoolMode != 0 {
		result.School = &tickets.Ticket_Submit_School{
			TestsTaken:  uint32(sub.Taken),
			TestsPassed: uint32(sub.Passed),
		}
	} else {
		var description string
		var test int64
		err := db.QueryRow("select ResultDesc.Description, Results.Test from Results, ResultDesc where "+
			"Results.UID = ? and ResultDesc.ID = Results.Result and not ResultDesc.Success order by Results.Test",
			sub.TestingID).Scan(&description, &test)
		switch {
		case err == sql.ErrNoRows:
			if sub.Passed != 0 && sub.Passed == sub.Taken {
				result.Acm = &tickets.Ticket_Submit_ACM{Result: "ACCEPTED"}
			}
		case err != nil:
			log.Fatalf("reading last test failure: %v", err)
			return nil
		default:
			result.Acm = &tickets.Ticket_Submit_ACM{Result: description, TestId: uint32(test)}
		}
	}
	return &result
}

func processSubmit(db *sqlx.DB, sender func(msg proto.Message) error, rows hasScanx) error {
	sub, err := scanSubmit(rows)
	if err != nil {
		log.Errorf("scan error: %v", err)
		return err
	}
	log.Debugf("processSubmit: %s", sub)
	related, err := findRelatedSubmits(db, sub)
	if err != nil {
		return err
	}

	result := tickets.Ticket{
		SubmitId:  uint32(sub.ID),
		Printer:   sub.Printer,
		Computer:  &tickets.Computer{Id: sub.ComputerID, Name: sub.ComputerName},
		Area:      &tickets.IdName{Id: uint32(sub.AreaID), Name: sub.AreaName},
		Contest:   &tickets.IdName{Id: uint32(sub.Contest), Name: sub.ContestName},
		Problem:   &tickets.Ticket_Problem{Id: sub.Task, Name: sub.ProblemName},
		JudgeTime: uint64(sub.Touched.UnixNano() / 1000),
		Submit:    make([]*tickets.Ticket_Submit, 0, len(related)),
	}

	teamName := sub.SchoolName
	if sub.TeamNum.Valid && sub.TeamNum.Int64 > 0 {
		teamName = teamName + " #" + strconv.FormatInt(sub.TeamNum.Int64, 10)
	}
	result.Team = &tickets.IdName{Id: uint32(sub.Team), Name: teamName}

	result.Submit = append(result.Submit, createSubmit(db, sub, len(related)+1))
	for count := len(related); count > 0; {
		count -= 1
		result.Submit = append(result.Submit, createSubmit(db, related[count], count))
	}

	if err = sender(&result); err != nil {
		return err
	}
	if _, err = db.Exec("Update Submits set Printed = Touched where ID = ?", sub.ID); err != nil {
		return err
	}
	log.Infof("Printed submit %d", sub.ID)
	return nil
}

type grserver struct {
	db *sqlx.DB
}

func (g grserver) processIncoming(conn *printserver.ServerConn, msg *stomp.Message) error {
	var ticket submitTicket
	if err := json.Unmarshal(msg.Body, &ticket); err != nil {
		log.Printf("Received malformed job: %s", err)
		return err
	}

	rows, err := g.db.Queryx(submitById, ticket.Submit.Id)
	if err != nil {
		log.Printf("Error looking up submit: %s", err)
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		if err = processSubmit(g.db, conn.Send, rows); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	flag.Parse()

	config, err := tools.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}

	var gs grserver
	gs.db, err = tools.CreateDb(config.Server.Db)
	if err != nil {
		log.Fatal(err)
	}

	pserver := printserver.Server{
		Source:      "/amq/queue/contester.tickets",
		Destination: "/amq/queue/ticket_pb",
	}

	pserver.StompConfig = &config.Messaging
	if err != nil {
		log.Fatalf("Opening stomp connection: %s", err)
		return
	}

	for {
		log.Println(pserver.Process(gs.processIncoming))
		time.Sleep(15 * time.Second)
	}
}
