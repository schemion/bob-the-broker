package main

import (
	httpapi "bob-the-broker/internal/api/http"
	"bob-the-broker/internal/broker"
	"log"
	"os"
)

func main() {
	b := broker.NewBroker()

	port := os.Getenv("PORT")
	if port == "" {
		port = "9092"
	}

	handler := httpapi.NewHandler(b)
	server := httpapi.NewServer(":"+port, handler.Routes())

	log.Println("Server started on port", port)
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
