package upsert_test

import (
	"flag"
	"fmt"
	"log"
	"testing"

	"github.com/brnstz/upsert"
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
	`,
		*host, *port, *user, *pw,
	)

	db, err = sqlx.Connect("postgres", opts)
	if err != nil {
		panic(err)
	}

	// try to drop db, ignore any errors (probably didn't exist)
	_, err = db.Exec(fmt.Sprintf(`DROP DATABASE "%s"`, *dbname))
	if err != nil {
		log.Println("couldn't drop database", err)
	}

	// Create the database
	_, err = db.Exec(fmt.Sprintf(`CREATE DATABASE "%s"`, *dbname))
	if err != nil {
		panic(err)
	}

	// Append new db name to opts
	opts += fmt.Sprintf(`dbname='%s'`, *dbname)

	db, err = sqlx.Connect("postgres", opts)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`
		CREATE TABLE person (
			id BIGSERIAL PRIMARY KEY,
			name TEXT,
			age INT
		)
	`)

	if err != nil {
		panic(err)
	}
}

type Person struct {
	Name string
	Age  int
	Id   int `upsert:"key"`
}

func (p *Person) Table() string {
	return "person"
}

func NewPerson(name string, age int) (p *Person, err error) {
	p = &Person{
		Name: name,
		Age:  age,
	}

	return
}

func GetPersonById(db sqlx.Ext, id int) (p *Person, err error) {
	p = &Person{}

	err = sqlx.Get(db, p, `SELECT * FROM person WHERE id = $1`, id)

	return
}

func TestUpsert(t *testing.T) {
	var err error

	p1, err := NewPerson("Brian Seitz", 36)
	if err != nil {
		t.Fatal(err)
	}

	_, err = upsert.Upsert(db, p1)
	if err != nil {
		t.Fatal(err)
	}

	p2, err := GetPersonById(db, p1.Id)
	if err != nil {
		t.Fatal(err)
	}

	p2.Age = 37
	_, err = upsert.Upsert(db, p2)
	if err != nil {
		t.Fatal(err)
	}

	p3, err := GetPersonById(db, p1.Id)
	if err != nil {
		t.Fatal(err)
	}

	if p3.Age != 37 {
		t.Fatalf("expected age 37 but got %d", p3.Age)
	}

}

func TestGet(t *testing.T) {
	p1, err := NewPerson("Steven Seagal", 64)
	if err != nil {
		t.Fatal(err)
	}

	_, err = upsert.Upsert(db, p1)
	if err != nil {
		t.Fatal(err)
	}

	// p2 should update without any write info
	p2, err := NewPerson("Steven Seagal", 64)
	if err != nil {
		t.Fatal(err)
	}

	status, err := upsert.Upsert(db, p2)
	if err != nil {
		t.Fatal(err)
	}

	if status != upsert.NoChange {
		t.Fatal("expected upsert.NoChange but got", status)
	}

	// p3 should update without any write info
	p3, err := NewPerson("Steven Seagal", 65)
	if err != nil {
		t.Fatal(err)
	}

	status, err = upsert.Upsert(db, p3)
	if err != nil {
		t.Fatal(err)
	}

	if status != upsert.Updated {
		t.Fatal("expected upsert.Updated but got", status)
	}

}
