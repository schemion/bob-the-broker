package main

import (
	grpcapi "bob-the-broker/internal/api/grpc"
	"bob-the-broker/internal/broker"
	"bob-the-broker/internal/brokerpb"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	b := broker.NewBroker()

	port := os.Getenv("PORT")
	if port == "" {
		port = "9092"
	}

	grpcServer := grpc.NewServer()
	grpcSrv := grpcapi.NewGrpcBroker(b)

	brokerpb.RegisterBrokerServiceServer(grpcServer, grpcSrv)
	reflection.Register(grpcServer)

	log.Println("Server started on port", port)
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer.Serve(lis)
}
