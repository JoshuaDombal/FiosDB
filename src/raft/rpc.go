package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)


// TODO: what is written here is more of a skeleton of how we could do it. Should we explore something like gRPC? Or
//    how can we avoid creating/re-creating tcp/tls connections
type RPC struct {
	serverPort  int
	idToPeerMap map[int]Peer
	aeChan      chan AppendEntriesRequestWrapper
	rvChan      chan RequestVoteRequestWrapper

}

func NewRPC(id int, idToPeerMap map[int]Peer) *RPC {
	rpc := &RPC{
		serverPort:  idToPeerMap[id].port,
		idToPeerMap: idToPeerMap,
		aeChan:      make(chan AppendEntriesRequestWrapper),
		rvChan:      make(chan RequestVoteRequestWrapper),
	}

	router := mux.NewRouter()
	router.HandleFunc("/append-entries", func(w http.ResponseWriter, req *http.Request) {
		bodyBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var aeRequest AppendEntriesRequest
		err = json.Unmarshal(bodyBytes, &aeRequest)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		rpc.aeChan<-AppendEntriesRequestWrapper{
			req: aeRequest,
			w:   w,
		}
	}).Methods(http.MethodPost)
	router.HandleFunc("/request-vote", func(w http.ResponseWriter, req *http.Request) {
		bodyBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var rvRequest RequestVoteRequest
		err = json.Unmarshal(bodyBytes, &rvRequest)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		rpc.rvChan<-RequestVoteRequestWrapper{
			req: rvRequest,
			w:   w,
		}
	}).Methods(http.MethodPost)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", rpc.serverPort), router))

	return rpc
}

func (r *RPC) SendAppendEntriesRequest(id int, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	peer := r.idToPeerMap[id]
	requestJson, err := json.Marshal(request)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	res, err := http.Post(fmt.Sprintf("%s:%d/append-entries", peer.ipAddr, peer.port), "application/json", bytes.NewBuffer(requestJson))
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	var aeResponse AppendEntriesResponse
	bodyBytes, err := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(bodyBytes, &aeResponse)
	return aeResponse, err
}

func (r *RPC) SendAppendEntriesResponse(w http.ResponseWriter, response AppendEntriesResponse) {
	responseJson, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	_, err = w.Write(responseJson)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (r *RPC) SendRequestVoteRequest(id int, request RequestVoteRequest) (RequestVoteResponse, error) {
	peer := r.idToPeerMap[id]
	requestJson, err := json.Marshal(request)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	res, err := http.Post(fmt.Sprintf("%s:%d/request-vote", peer.ipAddr, peer.port), "application/json", bytes.NewBuffer(requestJson))
	if err != nil {
		return RequestVoteResponse{}, err
	}

	var rvResponse RequestVoteResponse
	bodyBytes, err := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(bodyBytes, &rvResponse)
	return rvResponse, err
}

func (r *RPC) SendRequestVoteResponse(w http.ResponseWriter, response RequestVoteResponse) {
	responseJson, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	_, err = w.Write(responseJson)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

