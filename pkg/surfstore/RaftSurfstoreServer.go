package surfstore

import (
	context "context"
	"fmt"

	//"log"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	lastApplied    int64
	commitIndex    int64
	pendingCommits []*chan bool

	// myAddr    string
	peers []string
	id    int64
	ip    string

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (server *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	server.isLeaderMutex.RLock()
	isLeader := server.isLeader
	server.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		majorityAlive, _ := server.SendHeartbeat(ctx, empty)
		if majorityAlive.Flag {
			break
		}
	}
	return server.metaStore.GetFileInfoMap(ctx, empty)
}

func (server *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	server.isLeaderMutex.RLock()
	isLeader := server.isLeader
	server.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	for {
		majorityAlive, _ := server.SendHeartbeat(ctx, &emptypb.Empty{})
		if majorityAlive.Flag {
			break
		}
	}
	return server.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (server *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	server.isLeaderMutex.RLock()
	isLeader := server.isLeader
	server.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		majorityAlive, _ := server.SendHeartbeat(ctx, empty)
		if majorityAlive.Flag {
			break
		}
	}
	return server.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (server *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	op := UpdateOperation{
		Term:         server.term,
		FileMetaData: filemeta,
	}
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	server.isLeaderMutex.RLock()
	isLeader := server.isLeader
	server.isLeaderMutex.RUnlock()
	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	server.log = append(server.log, &op)
	committed := make(chan bool)
	server.pendingCommits = append(server.pendingCommits, &committed)

	go server.sendToAllFollowersInParallel()

	success := <-committed
	if success {
		server.lastApplied = server.commitIndex
		return server.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, ERR_SERVER_CRASHED
}

func (server *RaftSurfstore) sendToAllFollowersInParallel() {
	targetIdx := server.commitIndex + 1
	pendingIdx := int64(len(server.pendingCommits) - 1)
	commitChan := make(chan bool, len(server.peers))
	for idx, addr := range server.peers {
		if int64(idx) == server.id {
			continue
		}
		go server.sendToFollower(addr, targetIdx, commitChan)
	}

	commitCount := 1
	for {
		commit := <-commitChan
		server.isCrashedMutex.RLock()
		isCrashed := server.isCrashed
		server.isCrashedMutex.RUnlock()
		if isCrashed {
			*server.pendingCommits[pendingIdx] <- false
			break
		}
		if commit {
			commitCount++
		}
		if commitCount > len(server.peers)/2 {
			server.commitIndex = targetIdx
			*server.pendingCommits[pendingIdx] <- true
			break
		}
	}

}

func (server *RaftSurfstore) sendToFollower(addr string, entryIdx int64, commitChan chan bool) {
	for {
		server.isCrashedMutex.RLock()
		isCrashed := server.isCrashed
		server.isCrashedMutex.RUnlock()
		if isCrashed {
			commitChan <- false
		}
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64
		if entryIdx == 0 {
			prevLogTerm = 0
		} else {
			prevLogTerm = server.log[entryIdx-1].Term
		}
		input := &AppendEntryInput{
			Term:         server.term,
			PrevLogIndex: entryIdx - 1,
			PrevLogTerm:  prevLogTerm,
			Entries:      server.log[:entryIdx+1],
			LeaderCommit: server.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)

		if output != nil {
			if output.Success {
				commitChan <- true
				return
			}
		}
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (server *RaftSurfstore) AppendEntries(ctx context.Context, appendEntryInput *AppendEntryInput) (*AppendEntryOutput, error) {
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return &AppendEntryOutput{
			Success:      false,
			MatchedIndex: -1,
		}, ERR_SERVER_CRASHED
	}

	if appendEntryInput.Term > server.term {
		server.term = appendEntryInput.Term
		server.isLeaderMutex.Lock()
		server.isLeader = false
		server.isLeaderMutex.Unlock()
	}

	if appendEntryInput.Term < server.term {
		return &AppendEntryOutput{
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	for index, logEntry := range server.log {
		server.lastApplied = int64(index - 1)
		if len(appendEntryInput.Entries) < index+1 {
			server.log = server.log[:index]
			appendEntryInput.Entries = make([]*UpdateOperation, 0)
			break
		}
		if logEntry != appendEntryInput.Entries[index] {
			server.log = server.log[:index]
			appendEntryInput.Entries = appendEntryInput.Entries[index:]
			break
		}
		if len(server.log) == index+1 {
			appendEntryInput.Entries = appendEntryInput.Entries[index+1:]
		}
	}

	server.log = append(server.log, appendEntryInput.Entries...)

	if appendEntryInput.LeaderCommit > server.commitIndex {
		server.commitIndex = int64(math.Min(float64(appendEntryInput.LeaderCommit), float64(len(server.log)-1)))

		for server.lastApplied < server.commitIndex {
			server.lastApplied++
			entry := server.log[server.lastApplied]
			server.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}
	return &AppendEntryOutput{
		Success:      true,
		MatchedIndex: -1,
	}, nil

}

// This should set the leader status and any related variables as if the node has just won an election

func (server *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	server.term = server.term + 1
	server.isLeaderMutex.Lock()
	server.isLeader = true
	server.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (server *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	server.isCrashedMutex.RLock()
	isCrashed := server.isCrashed
	server.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	server.isLeaderMutex.RLock()
	isLeader := server.isLeader
	server.isLeaderMutex.RUnlock()
	if !isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	live := false
	up := 1
	for idx, addr := range server.peers {
		if int64(idx) == server.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return &Success{Flag: false}, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryappendEntryInput from server.nextIndex, etc
		var prevLogTerm int64
		if server.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = server.log[server.commitIndex].Term
		}
		appendEntryInput := &AppendEntryInput{
			Term:         server.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: server.commitIndex,
			Entries:      server.log,
			LeaderCommit: server.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, appendEntryInput)
		if output != nil {
			up++
			if up > len(server.peers)/2 {
				live = true
			}
		}
	}
	return &Success{Flag: live}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, err := s.metaStore.GetFileInfoMap(ctx, empty)
	if err != nil {
		fmt.Println("Error while getting fileinfomap - ", err)
		return nil, err
	}
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
