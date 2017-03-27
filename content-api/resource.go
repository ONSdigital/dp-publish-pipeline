package content

import (
	"database/sql"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/ONSdigital/dp-publish-pipeline/s3"
	"github.com/ONSdigital/dp-publish-pipeline/utils"
)

func GetResource(w http.ResponseWriter, r *http.Request, st *sql.Stmt) {
	bucketName := utils.GetEnvironmentVariable("S3_BUCKET", "content")
	endpoint := utils.GetEnvironmentVariable("S3_URL", "localhost:4000")
	accessKeyID := utils.GetEnvironmentVariable("S3_ACCESS_KEY", "1234")
	secretAccessKey := utils.GetEnvironmentVariable("S3_SECRET_ACCESS_KEY", "1234")

	uri := r.URL.Query().Get("uri")
	lang := r.URL.Query().Get("lang")
	if lang == "" {
		lang = "en"
	}
	results := st.QueryRow(uri + "?lang=" + lang)
	var s3Location sql.NullString
	notFound := results.Scan(&s3Location)
	if notFound != nil {
		log.Printf("Resource not found. uri : %s, language : %s, %s", uri, lang, notFound.Error())
	} else {
		s3Client := s3.CreateClient(bucketName, endpoint, accessKeyID, secretAccessKey, false)
		s3uri := strings.TrimLeft(s3Location.String, "s3://"+bucketName)
		data, err := s3Client.GetObject(s3uri)
		if err != nil {
			log.Printf("Resource not found uri %s, %s", uri, err.Error())
			http.Error(w, "Content not found", http.StatusNotFound)
		}
		w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(s3uri))
		w.Write(data)
	}
}
