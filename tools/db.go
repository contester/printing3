package tools

import "github.com/jmoiron/sqlx"

func CreateDb(spec string) (*sqlx.DB, error) {
	return sqlx.Connect("mysql", spec+"?parseTime=true&charset=utf8mb4,utf8")
}
