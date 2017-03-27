package content

import (
	"io/ioutil"
	"log"
	"net/http"
)

func GetParent(w http.ResponseWriter, r *http.Request) {
	log.Printf("Get parent")
	data, err := ioutil.ReadFile("static-parent.json")
	if err != nil {
		log.Printf("Error loading static file : %s", err.Error())
	}
	w.Write(data)
}
