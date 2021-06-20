package main


import (
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

var inMemoryDB = make(map[string]string)

type SetRequest struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/{key}", Get).Methods("GET")
	r.HandleFunc("/", Set).Methods("POST")
	r.HandleFunc("/{key}", Delete).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value, ok := inMemoryDB[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_, err := w.Write([]byte(value))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func Set(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request SetRequest
	err = json.Unmarshal(bodyBytes, &request)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if request.Key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	inMemoryDB[request.Key] = request.Value
}


func Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	delete(inMemoryDB, key)
}
