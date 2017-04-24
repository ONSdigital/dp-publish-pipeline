package content

import (
	"io/ioutil"
	"log"
	"net/http"
)

func GetTaxonomy(w http.ResponseWriter, r *http.Request, file string) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Printf("Error loading static file : %s", err.Error())
	}
	w.Write([]byte(data))
}
