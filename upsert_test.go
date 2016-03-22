package upsert_test

import (
	"flag"
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var (
	db *sqlx.DB
)

func init() {
	var err error

	host := flag.String("host", "localhost", "database host")
	port := flag.Int("port", 5432, "database port")
	user := flag.String("username", "postgres", "database username")
	pw := flag.String("password", "", "database password")
	dbname := flag.String("dbname", "upsert_test", "database name (warning: any existing database with this name will be dropped before the test)")
	flag.Parse()

	opts := fmt.Sprintf(
		"host='%s' port='%d' user='%s' password='%s' sslmode=disable dbname='%s'",
		*host, *port, *user, *pw, *dbname,
	)

	db, err = sqlx.Connect("postgres", opts)
	if err != nil {
		panic(err)
	}
}

func TestUpsert(t *testing.T) {
	var count int

	err := sqlx.Get(db, &count, "SELECT COUNT(*) FROM hello")
	if err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Fatalf("expected 1 but got %d", count)
	}
}
