package main

import (
    "log"
    "Sava/worker"
)


func main(){
    Worker, err := worker.NewWorker()
    if err != nil {
        log.Println("Can not create worker")
        return
    }

    
    go Worker.HandleInput()
    go Worker.WorkerMessageListener()
    go Worker.WorkerTaskListener()
    Worker.HeartBeat()
    
    
}