package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		log.Println("EOF while reading file. returning: ", err)
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	// CHECK
	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		ip:       config.RaftAddrs[id],
		ipList:   config.RaftAddrs,
		serverId: id,

		commitIndex: -1,
		lastApplied: -1,

		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	l, err := net.Listen("tcp", server.ip)
	if err != nil {
		return fmt.Errorf("failed to listen %v", err)
	}
	if err := grpcServer.Serve(l); err != nil {
		return fmt.Errorf("failed to serve %v", err)
	}
	return nil
}
