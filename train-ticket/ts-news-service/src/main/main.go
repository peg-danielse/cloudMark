package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

func hello(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `[{"Title":"News Service Complete","Content":"Congratulations:Your News Service Complete"},{"Title":"Total Ticket System Complete","Content":"Just a total test"}]`)
}

func main() {
	port := flag.Int("advertise-port", 12862, "Port this service listens on and advertises")
	flag.Parse()
	http.HandleFunc("/", hello)
	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	log.Fatal(http.ListenAndServe(addr, nil))
}
