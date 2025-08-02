package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"sync"
)

type Key string
type NodeAddress string

type Node struct {
	mu          sync.Mutex
	Address     NodeAddress
	FingerTable []NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress
	Bucket      map[Key]string
	PK_A, PK_B  int64
}

func (n *Node) create() {
	n.mu.Lock()
	n.Predecessor = ""
	n.Successors = append(n.Successors, n.Address)
	n.mu.Unlock()
}

func (n *Node) HandlePing(arguments *Args, reply *Reply) error {
	n.mu.Lock()
	if arguments.Command == "CP" {
		reply.Reply = "CP reply"
	}
	n.mu.Unlock()
	return nil
}

func Inbetween(start *big.Int, elt *big.Int, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

func (n *Node) Get_predecessor(args *Args, reply *Reply) error {
	n.mu.Lock()
	reply.Reply = string(node.Predecessor)
	n.mu.Unlock()
	return nil

}

func (n *Node) closest_preceding_node(id *big.Int) NodeAddress {
	for i := len(n.FingerTable) - 1; i >= 0; i-- {
		addH := hashAddress(n.Address)
		fingerH := hashAddress(n.FingerTable[i])

		if Inbetween(addH, fingerH, id, true) {
			return n.FingerTable[i]
		}
	}
	return n.Successors[0]
}

func hashAddress(elt NodeAddress) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))

	t := new(big.Int).SetBytes(hasher.Sum(nil))

	return new(big.Int).Mod(t, big.NewInt(int64(1024)))
}

func (n *Node) FindSuccessor(args *Args, reply *Reply) error {
	n.mu.Lock()
	addH := hashAddress(n.Address)

	ID := hashAddress(NodeAddress(args.Address))
	ID.Add(ID, big.NewInt(args.Offset))
	ID.Mod(ID, big.NewInt(int64(math.Pow(2, float64(FingerTableSize)))))

	successor_Hash := hashAddress(NodeAddress(n.Successors[0]))

	//If the ID is between self and immediate successor
	if Inbetween(addH, ID, successor_Hash, false) {
		reply.Found = true
		reply.Reply = string(n.Successors[0])
	} else {
		//if the file is outside. Should return the closest preceding node before ID. Have to implement fix_fingers for this to work.
		//Right now it will return the next successor, jumping only 1 step on the ring. Search time is O(N), we want O(log(N))
		reply.Found = false
		reply.Forward = string(n.closest_preceding_node(ID))
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) Get_successors(args *Args, reply *Reply) error {
	n.mu.Lock()
	reply.Successors = node.Successors
	n.mu.Unlock()
	return nil
}

func (n *Node) join(address NodeAddress) {
	n.mu.Lock()
	node.Predecessor = ""
	node.Successors = []NodeAddress{address}
	n.mu.Unlock()
}

func (n *Node) Notify(args *Args, reply *Reply) error {
	n.mu.Lock()
	addH := hashAddress(NodeAddress(args.Address))

	addressH := hashAddress(n.Address)

	preH := hashAddress(NodeAddress(n.Predecessor))

	if n.Predecessor == "" || (Inbetween(preH, addH, addressH, false)) {
		n.Predecessor = NodeAddress(args.Address)
		reply.Reply = "Success"
	} else {
		reply.Reply = "Fail"
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) Store(args *Args, reply *Reply) error {
	filename := args.Filename
	content := []byte(args.Command)

	//if the file is to be stored locally then there is no need to make a call
	if hashAddress(NodeAddress(add)) == hashAddress(node.Address) {
		return nil
	}

	err := os.WriteFile(filename, []byte(content), 0777)
	if err != nil {
		fmt.Println("problem writing file")
	}
	return nil
}

func (n *Node) GetFile(args *Args, reply *Reply) error {
	f, err := os.Open(args.Filename)
	if err != nil {
		return nil
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return nil
	}

	reply.Content = string(content)
	return nil
}
