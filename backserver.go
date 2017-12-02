package main

import (
	"SDFS/shareReadWrite"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func main() {
	rpc.Register(shareReadWrite.NewNode("localhost:9876", "localhost:10030"))
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":9876")
	if err != nil {
		log.Fatal("listen error:", err)

	}
	for {
		http.Serve(l, nil)
	}
}
