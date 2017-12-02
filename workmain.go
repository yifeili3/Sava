package main

import (
    "fmt"
    "log"
    "Sava/worker"
)


func main(){
    Worker, err := worker.NewWorker()
    if err != nil {
        log.Println("Can not create worker")
        return
    }

    go Worker.HeartBeat()
    go Worker.HandleInput()
    Worker.WorkerTaskListener()
    
}