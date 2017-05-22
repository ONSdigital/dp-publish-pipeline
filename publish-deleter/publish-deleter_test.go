package main

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/ONSdigital/go-ns/log"
	. "github.com/smartystreets/goconvey/convey"
)

func TestInvalidJson(t *testing.T) {
	t.Parallel()
	db, errPostgres := createPostgresConnection()
	elasticClient, errElastic := createElasticSearchClient()
	if errPostgres == nil || errElastic == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		defer deleteStmt.Close()
		producerChannel := make(chan []byte, 1)
		Convey("With a invalid json message, error code is returned", t, func() {
			publishErr := publishDelete([]byte("{one: two"), deleteStmt, elasticClient, producerChannel)
			So(publishErr, ShouldNotBeNil)
		})
	} else {
		t.Skip("Local postgres database or elastic search was not found")
	}
}

func TestMissinJsonParameters(t *testing.T) {
	t.Parallel()
	db, errPostgres := createPostgresConnection()
	elasticClient, errElastic := createElasticSearchClient()
	if errPostgres == nil || errElastic == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		defer deleteStmt.Close()
		producerChannel := make(chan []byte, 1)
		Convey("With a missing json parameters, error code is returned", t, func() {
			err := publishDelete([]byte("{\"CollectionId\":\"three\", \"ScheduleId\":0, \"DeleteId\":0 }"), deleteStmt, elasticClient, producerChannel)
			So(err, ShouldNotBeNil)
			err = publishDelete([]byte("{\"ScheduleId\":0, \"DeleteId\":0, \"Uri\": \"two\" }"), deleteStmt, elasticClient, producerChannel)
			So(err, ShouldNotBeNil)
		})
	} else {
		t.Skip("Local postgres database or elastic search was not found")
	}
}

// The following tests can't be ran in parallel as they test if data is in
// postgres or not.

func TestRowIsRemovedFromPostgres(t *testing.T) {
	db, errPostgres := createPostgresConnection()
	elasticClient, errElastic := createElasticSearchClient()
	if errPostgres == nil || errElastic == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		defer deleteStmt.Close()
		producerChannel := make(chan []byte, 1)
		Convey("With valid message, both english and welsh content is removed", t, func() {
			So(AddTestData(db), ShouldBeNil)
			message := []byte("{\"CollectionId\":\"123\", \"ScheduleId\":0, \"DeleteId\":0,\"Uri\": \"/aboutus\"}")
			err := publishDelete(message, deleteStmt, elasticClient, producerChannel)
			So(err, ShouldBeNil)
			rowsAffected, sqlErr := FindTestData(db)
			So(sqlErr, ShouldBeNil)
			So(rowsAffected, ShouldBeZeroValue)
		})
	} else {
		t.Skip("Local postgres database or elastic search was not found")
	}
}

func TestProducerMessage(t *testing.T) {
	db, errPostgres := createPostgresConnection()
	elasticClient, errElastic := createElasticSearchClient()
	if errPostgres == nil || errElastic == nil {
		defer db.Close()
		deleteStmt := prepareSQLDeleteStatement(db)
		defer deleteStmt.Close()
		producerChannel := make(chan []byte, 1)
		Convey("With content succesfully removed, a complete message is sent ", t, func() {
			So(AddTestData(db), ShouldBeNil)
			message := []byte("{\"CollectionId\":\"123\", \"ScheduleId\":43, \"DeleteId\":34,\"Uri\": \"/aboutus\"}")
			err := publishDelete(message, deleteStmt, elasticClient, producerChannel)
			So(err, ShouldBeNil)
			kafkaMessage := string(<-producerChannel)
			So(kafkaMessage, ShouldContainSubstring, "/aboutus")
			So(kafkaMessage, ShouldContainSubstring, "123")
			So(kafkaMessage, ShouldContainSubstring, "43")
			So(kafkaMessage, ShouldContainSubstring, "34")
		})
	} else {
		t.Skip("Local postgres database or elastic search was not found")
	}
}

func AddTestData(db *sql.DB) error {
	insertContentSQL := "INSERT INTO metadata(collection_id, uri, content) VALUES('123', $1, '{}')"
	statement, err := db.Prepare(insertContentSQL)
	if err != nil {
		log.ErrorC("Could not prepare statement on database", err, nil)
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
		log.ErrorC("Could not prepare statement on database", err, nil)
	}
	defer statement.Close()
	row, sqlErr := statement.Exec()
	if sqlErr != nil {
		return 0, sqlErr
	}

	rowsAffected, rowErr := row.RowsAffected()
	log.Info(fmt.Sprintf("%d", rowsAffected), nil)
	return rowsAffected, rowErr
}
