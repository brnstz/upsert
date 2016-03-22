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
	var (
		err    error
		host   *string = flag.String("host", "localhost", "database host")
		user           = flag.String("user", "postgres", "database user")
		pw             = flag.String("pw", "", "database password")
		dbname         = flag.String("db", "upsert_test", "db name")
		port   *int    = flag.Int("port", 5432, "database port")
	)
	flag.Parse()

	opts := fmt.Sprintf(`
		host='%s' 
		port='%d' 
		user='%s' 
		password='%s' 
		sslmode=disable 
		dbname='%s'
	`,
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
