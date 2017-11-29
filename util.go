package util

type Message struct{
    MsgType    string // Start/Superstep
    SuperStep  int
    Partition  []MetaInfo
}

type MetaInfo struct{
    ID          int
    StartNode   int
    EndNode     int
}