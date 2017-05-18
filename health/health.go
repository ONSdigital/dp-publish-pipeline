package health

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type healthMessage struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func NewHealthChecker(healthChannel chan bool, dbStmt *sql.Stmt) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			healthIssue string
			err         error
		)

		// assume all well
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		body := []byte("{\"status\":\"OK\"}") // quicker than json.Marshal(healthMessage{...})

		// test main loop
		if healthChannel != nil {
			healthChannel <- true
		}

		// test db access
		if dbStmt != nil {
			_, err = dbStmt.Exec()
			if err != nil {
				healthIssue = err.Error()
			}
		}

		// when there's a healthIssue, change headers and content
		if healthIssue != "" {
			w.WriteHeader(http.StatusInternalServerError)
			if body, err = json.Marshal(healthMessage{
				Status: "error",
				Error:  healthIssue,
			}); err != nil {
				log.Panic(err)
			}
		}

		// return json
		fmt.Fprintf(w, string(body))
	}
}
