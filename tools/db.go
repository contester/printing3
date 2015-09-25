package tools

import "github.com/jmoiron/sqlx"

func CreateDb(spec string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("mysql", spec+"?parseTime=true&charset=utf8mb4,utf8")
	if err != nil {
		return nil, err
	}
	return db, nil
}
