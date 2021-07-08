package main


import (
	"./bplustree"
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

var db = bplustree.New(1, 3)

type SetRequest struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/{key}", Get).Methods("GET")
	r.HandleFunc("/", Set).Methods("POST")
	// r.HandleFunc("/{key}", Delete).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func Get(w http.ResponseWriter, r *http.Request) {
	log.Printf("Handling get request: %v\n", r)
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value, ok := db.Get(key)
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
	log.Printf("Handling set request: %v\n", r)
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

	db.Set(request.Key, request.Value)
}


// func Delete(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	key, ok := vars["key"]
// 	if !ok {
// 		w.WriteHeader(http.StatusBadRequest)
// 		return
// 	}
// 	db.Delete(key)
// }
