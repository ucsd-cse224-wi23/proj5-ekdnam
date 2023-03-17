package surfstore

import (
	context "context"
	"fmt"
	"log"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	myBlock, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	// log.Println("Block:", string(block.BlockData))
	block.BlockData = myBlock.BlockData
	block.BlockSize = myBlock.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	log.Println(blockStoreAddr)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// log.Println("BlockStore client initiated")

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		log.Println("Error occurred while putting: ", err.Error())
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	BlockHashesIn := &BlockHashes{Hashes: blockHashesIn}
	blockHashes, err := c.HasBlocks(ctx, BlockHashesIn)

	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashes.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the serverm
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// metaStoreAddr := surfClient.MetaStoreAddrs[0]
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		m := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := m.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*serverFileInfoMap = fileInfoMap.FileInfoMap
		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("cannot find leader. Cluster is down")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the serverm
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// metaStoreAddr := surfClient.MetaStoreAddrs[0]
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		m := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		version, err := m.UpdateFile(ctx, fileMetaData)
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				log.Printf("%s is crashed. Continuing\n", metaStoreAddr)
				continue
			} else if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				log.Printf("%s is not leader. Continuing\n", metaStoreAddr)
				continue
			}
			conn.Close()
			return err
		}
		*latestVersion = version.Version
		return conn.Close()
	}
	return fmt.Errorf("cannot find leader. Cluster is down")
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mblockHashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	*blockHashes = mblockHashes.Hashes
	// close the connection
	return conn.Close()

}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		m := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		addrs, err := m.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				log.Printf("%s is crashed. Continuing\n", metaStoreAddr)
				continue
			} else if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				log.Printf("%s is not leader. Continuing\n", metaStoreAddr)
				continue
			}
			conn.Close()
			return err
		}
		*blockStoreAddrs = addrs.BlockStoreAddrs
		return conn.Close()
	}
	return fmt.Errorf("cannot find leader. Cluster is down")
}

// Given a list of block hashes, find out which block server they belong to.
// Returns a mapping from block server address to block hashes.
func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		m := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		hashesIn := new(BlockHashes)
		hashesIn.Hashes = blockHashesIn
		myMap, err := m.GetBlockStoreMap(ctx, hashesIn)
		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		tempStoreMap := make(map[string][]string)
		for k, v := range myMap.BlockStoreMap {
			tempStoreMap[k] = v.Hashes
		}
		*blockStoreMap = tempStoreMap
		return conn.Close()
	}
	return fmt.Errorf("cannot find leader. Cluster is down")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
