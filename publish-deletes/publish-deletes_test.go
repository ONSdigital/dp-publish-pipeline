package main

import (
	"database/sql"
	"log"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestInvalidJson(t *testing.T) {
	t.Parallel()
	db, err := createPostgresConnection()
	if err == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		Convey("With a invalid json message, error code is returned", t, func() {
			err := publishDelete([]byte("{one: two"), deleteStmt)
			So(err, ShouldNotBeNil)
		})
	} else {
		log.Printf("Err : %s", err.Error())
		t.Skip("Local postgres database was not found")
	}
}

func TestMissinJsonParameters(t *testing.T) {
	t.Parallel()
	db, err := createPostgresConnection()
	if err == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		Convey("With a missing json parameters, error code is returned", t, func() {
			err := publishDelete([]byte("{\"CollectionId\":\"three\", \"ScheduleId\":0, \"DeleteId\":0 }"), deleteStmt)
			So(err, ShouldNotBeNil)
			err = publishDelete([]byte("{\"ScheduleId\":0, \"DeleteId\":0, \"DeleteLocation\": \"two\" }"), deleteStmt)
			So(err, ShouldNotBeNil)
		})
	} else {
		t.Skip("Local postgres database was not found")
	}
}

func TestRowIsRemovedFromPostgres(t *testing.T) {
	t.Parallel()
	db, err := createPostgresConnection()
	if err == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		Convey("With valid message, a row is removed from Postgres", t, func() {
			So(AddTestData(db), ShouldBeNil)
			err := publishDelete([]byte("{\"CollectionId\":\"123\", \"ScheduleId\":0, \"DeleteId\":0,\"DeleteLocation\": \"/aboutus\"}"), deleteStmt)
			So(err, ShouldBeNil)
			testDataFound, sqlErr := FindTestData(db)
			So(sqlErr, ShouldBeNil)
			So(testDataFound, ShouldBeZeroValue)
		})
	} else {
		t.Skip("Local postgres database was not found")
	}
}

func AddTestData(db *sql.DB) error {

	insertContentSQL := "INSERT INTO metadata(collection_id, uri, content) VALUES('123', $1, '{}')"
	statement, err := db.Prepare(insertContentSQL)
	if err != nil {
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
	}
	defer statement.Close()
	_, sqlErr := statement.Exec("/aboutus?lang=en")
	_, sqlErr = statement.Exec("/aboutus?lang=cy")

	return sqlErr
}

func FindTestData(db *sql.DB) (int64, error) {
	insertContentSQL := "SELECT * FROM metadata WHERE collection_id = '123' AND uri LIKE '/aboutus?lang=%'"
	statement, err := db.Prepare(insertContentSQL)
	if err != nil {
		log.Panicf("Error: Could not prepare statement on database: %s", err.Error())
	}
	defer statement.Close()
	row, sqlErr := statement.Exec()
	if sqlErr != nil {
		return 0, sqlErr
	}

	rowsAffected, rowErr := row.RowsAffected()
	log.Printf("%d", rowsAffected)
	return rowsAffected, rowErr
}
