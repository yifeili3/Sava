package worker

import (
    "fmt"
    "Sava/vertex"
	"net"
)


type Worker struct{
    Superstep   int
    ID          int
    vList       map[int]vertex.Vertex
    Filename    string
    Connection  *net.UDPConn
    Addr        net.UDPAddr
}

