package main

import (
	"./bplustree"
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

const cacheSize = 64

var bPlusTree = bplustree.NewBPlusTree("./data/db", cacheSize, -1)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/{key}", Get).Methods(http.MethodGet)
	r.HandleFunc("/", Set).Methods(http.MethodPost)
	r.HandleFunc("/{key}", Delete).Methods(http.MethodDelete)

	log.Fatal(http.ListenAndServe(":8080", r))
}

func Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Printf("Handling get request for key: %s\n", key)
	value, ok := bPlusTree.Get(key)
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

	log.Printf("Handling set request for key: %s, value: %s\n", request.Key, request.Value)
	if request.Key == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bPlusTree.Set(request.Key, request.Value)
}

func Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key, ok := vars["key"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Printf("Handling delete request for key: %s\n", key)
	bPlusTree.Delete(key)
}
