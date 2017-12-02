package shareReadWrite

import (
	"fmt"
	"io/ioutil"
	"os"
)

const (
	localpath = "/home/yifieli3/SDFS_local/"
	distpath  = "/home/yifeili3/SDFS_dist/"
)

type Node struct {
	Myaddress     string
	Targetaddress string
}

type WriteCmd struct {
	File  string
	Input string
}

func NewNode(myaddr string, taraddr string) *Node {
	return &Node{
		Myaddress:     myaddr,
		Targetaddress: taraddr,
	}
}

func (n *Node) ReadLocalFile(file string, ret *string) error {
	fmt.Println("ReadLocalFIle in  :" + file)
	fin, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("Remote read failure")
		return err
	}
	//fmt.Printf("Read %s success\nThe result is:%s", file, string(fin))
	if err != nil {

	} else {
		*ret = string(fin)
	}

	return err
}

func (n *Node) WriteLocalFile(cmd WriteCmd, ret *string) error {
	//fmt.Printf("In writeLocalFile: for file %s, the input is %s:\n", cmd.File, cmd.Input)
	err := ioutil.WriteFile(cmd.File, []byte(cmd.Input), 0666)

	if err != nil {
		fmt.Println("Remote Write failure")
		return err
	}
	fmt.Printf("Wirte file %s success\n", cmd.File)
	*ret = "WRITE"
	return err
}

func (n *Node) DeleteFile(file string, ret *string) error {
	err := os.Remove(file)
	checkError(err)
	*ret = "DELETE"
	return err
}

func checkError(err error) {
	if err != nil {
		fmt.Printf("my Error: %s\n", err.Error())
		//os.Exit(1)
	}
}
