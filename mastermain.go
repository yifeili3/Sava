package main

import (
	"Sava/master"
	"log"
	"time"
)

func main() {
	Master, err := master.NewMaster()
	if err != nil {
		log.Println(err)
		return
	}
	go Master.UDPListener()
	for {
		Master.UpdateMemberShip()
		time.Sleep(time.Millisecond * 100)
	}
}
