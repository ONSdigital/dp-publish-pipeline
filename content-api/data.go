package content

import (
	"database/sql"
	"log"
	"net/http"
)

func GetData(w http.ResponseWriter, r *http.Request, st *sql.Stmt) {
	uri := r.URL.Query().Get("uri")
	lang := r.URL.Query().Get("lang")
	if lang == "" {
		lang = "en"
	}
	results := st.QueryRow(uri + "?lang=" + lang)
	var content sql.NullString
	notFound := results.Scan(&content)
	if notFound != nil {
		log.Printf("Data not found. uri : %s, language : %s, %s", uri, lang, notFound.Error())
		http.Error(w, "Content not found", http.StatusNotFound)
	}
	w.Write([]byte(content.String))
}
