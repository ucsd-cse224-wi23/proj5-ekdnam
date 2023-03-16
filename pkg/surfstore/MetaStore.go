package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

// Returns a mapping of the files stored in the SurfStore cloud service, including the version, filename, and hashlist
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

// Updates the FileInfo values associated with a file stored in the cloud.
// This method replaces the hash list for the file with the provided hash list only if the new version number is
// exactly one greater than the current version number. Otherwise, you can send version=-1
// to the client telling them that the version they are trying to store is not right (likely too old).
func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fname := fileMetaData.Filename
	clientVersion := fileMetaData.Version
	var version *Version = new(Version)
	val, ok := m.FileMetaMap[fname]
	if !ok {
		m.FileMetaMap[fname] = fileMetaData
		*version = Version{Version: clientVersion}
		return version, nil
	} else {
		serverVersion := val.Version
		if clientVersion == serverVersion+1 {
			m.FileMetaMap[fname] = fileMetaData
			*version = Version{Version: clientVersion}
			return version, nil
		} else {
			*version = Version{Version: -1}
			return version, fmt.Errorf("file version mismatch")
		}
	}
}

// Given a list of block hashes, find out which block server they belong to.
// Returns a mapping from block server address to block hashes.
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	bsMap := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		if _, ok := bsMap[server]; !ok {
			bsMap[server] = &BlockHashes{}
		}
		bsMap[server].Hashes = append(bsMap[server].Hashes, hash)
	}
	blockStoreMap := &BlockStoreMap{BlockStoreMap: bsMap}
	return blockStoreMap, nil
}

// Returns all the BlockStore addresses.
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	blockStoreAddrs := &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}
	return blockStoreAddrs, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	consistentHashRing := NewConsistentHashRing(blockStoreAddrs)
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: consistentHashRing,
	}
}
