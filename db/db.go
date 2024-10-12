package db

import (
	_ "embed"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/lib/pq"
)

//go:embed schema.sql
var schemaSQL string

func OpenDB(dsn string) *sqlx.DB {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		log.Fatalln(err)
	}

	db.Mapper = reflectx.NewMapperFunc("json", strings.ToLower)

	db.MustExec(schemaSQL)

	return db
}

func OpenListener(dsn string) *pq.Listener {
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("[pg listener error] %v", err)
		}
	}

	return pq.NewListener(dsn, time.Second, time.Second*30, reportProblem)
}
