package main

import (
	"./bplustree"
	c "./constants"
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

// numKeys*keySize + (numKeys + 1)*pageRefSize + pageTypeSize + keyCountSize <= pageSize, where capacity = numKeys
const capacity = int((c.PageSize - c.PageTypeSize - c.KeyCountSize - c.KeySize) / (c.KeySize + c.PageRefSize))
const cacheSize = 64

var bPlusTree = bplustree.New("./data/db", capacity, cacheSize)

type SetRequest struct {
	Key   string `json:"key"`
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
