#!bin/sh
go build -o ../../pkg/worker.a ./worker/
go build -o ../../pkg/vertices.a ./vertices/
go build -o ../../pkg/master.a ./master/
go build -o ../../pkg/util.a ./util/
