package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

// Stores block b in the key-value store, indexed by hash value h
func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	log.Println("In BlockStore.go/GetBlock")
	val, ok := bs.BlockMap[blockHash.Hash]
	if !ok {
		return &Block{BlockData: nil, BlockSize: -1}, fmt.Errorf("BlockHash not present in Server. Please check different server")
	}
	return val, nil
}

// Retrieves a block indexed by hash value h
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	log.Println("In PutBlock")
	myBlock := &Block{BlockData: block.BlockData, BlockSize: int32(len(block.BlockData))}
	log.Println("MyBlock created")
	bs.BlockMap[GetBlockHashString(block.BlockData)] = myBlock
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashesPresent []string
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			hashesPresent = append(hashesPresent, hash)
		}
	}
	blockHashes := &BlockHashes{Hashes: hashesPresent}
	return blockHashes, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var hashesPresent []string
	for hash, _ := range bs.BlockMap {
		hashesPresent = append(hashesPresent, hash)
	}
	blockHashes := &BlockHashes{Hashes: hashesPresent}
	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
