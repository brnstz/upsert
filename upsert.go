package upsert

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

var (
	ErrNoIDReturned = errors.New("no id returned")
)

// Upserter is a interface specific to sqlx and PostgreSQL that can save
// a single row of data via Upsert(), Update() or Insert(). It doesn't try to
// know anything about relationships between tables.
type Upserter interface {
	// Table returns table name we should save to
	Table() string
}

// updateColumns returns the fields that are read from the struct and set
// on upserting in the db. Typically this should include everything except the
// key fields and any composite (array, nested struct) types or any
// field that doesn't map directly into a db column. Tag a field with
// `upsert:"omit"` to explicitly exclude from this list.
func updateColumns(u Upserter) (columns []string) {
	ut := reflect.TypeOf(u)

	if ut.Kind() == reflect.Ptr {
		ut = ut.Elem()
	}

	if ut.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < ut.NumField(); i++ {
		field := ut.Field(i)
		tag := field.Tag

		// Include any column that isn't tagged with upsert:omit
		if !strings.Contains(tag.Get("upsert"), "omit") {
			columns = append(columns, dbTagOrLower(field.Name, tag))
		}
	}

	return
}

// dbTagOrLower returns either the value of the "db" struct tag for this field
// or, if that does not exist, it returns a lowercase version of the field name.
func dbTagOrLower(name string, tag reflect.StructTag) string {
	dbTag := tag.Get("db")
	if len(dbTag) > 0 {
		return dbTag
	} else {
		return strings.ToLower(name)
	}
}

// uniqueKeyColumns returns the fields of the struct that together are
// naturally unique. For example, an md5 hash of the content. Or a
// foreign key plus an internal value. This is used in where clause
// when trying to find existing rows. Tag a field with `"upsert:"key"`
// to include in the unique key.
func uniqueKeyColumns(u Upserter) (columns []string) {
	ut := reflect.TypeOf(u)

	if ut.Kind() == reflect.Ptr {
		ut = ut.Elem()
	}

	if ut.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < ut.NumField(); i++ {
		field := ut.Field(i)
		tag := field.Tag
		// Check if upsert tag contains "key". This wouldn't work
		// if possible options were substrings of one another. For a
		// better implementation, look at src/encoding/json/tags.go
		if strings.Contains(tag.Get("upsert"), "key") {
			columns = append(columns, dbTagOrLower(field.Name, tag))
		}
	}

	return
}

// set returns a string like "SET "col1" = :col1, "col2" = :col2" for
// use with sqlx.NamedExec() and friends.
func set(u Upserter) string {
	cols := updateColumns(u)
	n := len(cols)

	b := bytes.Buffer{}

	b.WriteString("SET ")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `"%s" = :%s`, cols[i], cols[i])

		// If we are not at the last value, add a comma
		if i < n-1 {
			b.WriteRune(',')
		}
	}

	return b.String()
}

// values returns a string like `("col1", "col2") VALUES(:col1, :col2)`
// for use with sqlx.NamedExec() etc.
func values(u Upserter) string {
	cols := updateColumns(u)
	n := len(cols)

	b := bytes.Buffer{}

	b.WriteRune('(')
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `"%s"`, cols[i])

		// If we are not at the last value, add a comma
		if i < n-1 {
			b.WriteRune(',')
		}
	}
	b.WriteRune(')')

	b.WriteString("VALUES (")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `:%s`, cols[i])

		// If we are not at the last value, add a comma
		if i < n-1 {
			b.WriteRune(',')
		}
	}
	b.WriteRune(')')

	return b.String()
}

// where returns an SQL where clause with all the key columns of
// this Upserter
func where(u Upserter) string {
	keycols := uniqueKeyColumns(u)
	b := bytes.Buffer{}
	n := len(keycols)

	fmt.Fprintf(&b, "WHERE ")

	for i := 0; i < n; i++ {
		// If we need to support NULLs here, the best option may be
		// something like (x = y OR (x is null and y is null))
		// rather than "IS NOT DISTINCT FROM" which doesn't use indexes
		// it seems
		fmt.Fprintf(&b, `%s = :%s`, keycols[i], keycols[i])

		if i < n-1 {
			fmt.Fprint(&b, " AND ")
		}
	}

	return b.String()
}

// updateSQL returns a full SQL command to update this Upserter u
func updateSQL(u Upserter) string {
	q := fmt.Sprintf(`UPDATE "%s" %s %s RETURNING *`,
		u.Table(), set(u), where(u))

	return q
}

// insertSQL returns a full SQL command to insert this Upserter u
func insertSQL(u Upserter) string {
	q := fmt.Sprintf(`INSERT INTO "%s" %s RETURNING *`, u.Table(), values(u))
	return q
}

func Update(ext sqlx.Ext, u Upserter) (err error) {
	/*
		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Printf("update of %v took %v with err %v\n", u, t2.Sub(t1), err)
		}()
	*/

	// Try to update an existing row
	rows, err := sqlx.NamedQuery(ext, updateSQL(u), u)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(u)
		if err != nil {
			log.Println(err)
		}
	} else {
		// We could not find anything to update.
		err = ErrNoIDReturned
		return
	}

	return
}

// Insert takes either an sqlx.DB or sqlx.Tx as ext, along with a value
// that implements the Upserter() interface. We attempt to insert it
// and set its primary key id value.
func Insert(ext sqlx.Ext, u Upserter) (err error) {
	/*
		t1 := time.Now()
		defer func() {
			t2 := time.Now()
			log.Printf("insert of %v took %v\n", u, t2.Sub(t1))
		}()
	*/

	// Try to insert a row
	rows, err := sqlx.NamedQuery(ext, insertSQL(u), u)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(u)
		if err != nil {
			log.Println(err)
		}
	} else {
		// No rows were returned but no SQL error. Weird, return generic
		// error.
		err = ErrNoIDReturned
		return
	}

	return
}

// Upsert takes either an sqlx.DB or sqlx.Tx as ext, along with a value
// that implements the Upserter() interface. We attempt to insert/update it
// and set the new primary key id if that succeeds. inserted returns true
// if a new row was inserted. The client is responsible for wrapping
// in a transaction when needed. This can be used when running a transaction
// at a higher level (upserting multiple items).
func Upsert(ext sqlx.Ext, u Upserter) (inserted bool, err error) {
	// Try to update, return immediately if succcesful
	err = Update(ext, u)
	if err == nil {
		return
	}

	// Can't update? Try insert
	err = Insert(ext, u)
	if err != nil {
		log.Println(err)
		return
	}

	inserted = true

	return
}

// UpsertTx takes only an sqlx.DB and wraps the upsert attempt into a
// a transaction.
func UpsertTx(db *sqlx.DB, u Upserter) (inserted bool, err error) {
	tx, err := db.Beginx()
	if err != nil {
		log.Println("can't start transaction", err)
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Try to update
	err = Update(tx, u)

	// If we have a nil error, we successfully updated. If we have
	// an err other than ErrNoIDReturned, we couldn't update for an
	// unexpected reason. In either case return.
	if err != ErrNoIDReturned {
		return
	}

	// No ID returned in the update? Try insert
	err = Insert(tx, u)
	if err != nil {
		log.Println(err)
		return
	}

	inserted = true

	return
}

func Delete(ext sqlx.Ext, u Upserter) (err error) {
	q := fmt.Sprintf(`DELETE FROM "%s" %s`,
		u.Table(), where(u))
	_, err = sqlx.NamedExec(ext, q, u)

	if err != nil {
		log.Println("can't delete", err)
		return
	}

	return
}
