package main

import (
	httpapi "bob-the-broker/internal/api/http"
	"bob-the-broker/internal/broker"
	"log"
)

func main() {
	b := broker.NewBroker()

	handler := httpapi.NewHandler(b)
	server := httpapi.NewServer(":8080", handler.Routes())

	log.Println("Server started on :8080")
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
