package surfstore

import (
	context "context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
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

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) checkCrashed() bool {
	s.isCrashedMutex.RLock()
	crashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	return crashed
}

func (s *RaftSurfstore) checkLeader() bool {
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	s.isLeaderMutex.RUnlock()
	return leader
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	crashed := s.checkCrashed()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	leader := s.checkLeader()
	if !leader {
		return nil, ERR_NOT_LEADER
	}
	// check if majority of the servers are up
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	crashed := s.checkCrashed()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	leader := s.checkLeader()
	if !leader {
		return nil, ERR_NOT_LEADER
	}
	empty := &emptypb.Empty{}
	// check if majority of the servers are up
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	crashed := s.checkCrashed()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	leader := s.checkLeader()
	if !leader {
		return nil, ERR_NOT_LEADER
	}
	// check if majority of the servers are up
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}
	return s.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	crashed := s.checkCrashed()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	leader := s.checkLeader()
	if !leader {
		return nil, ERR_NOT_LEADER
	}
	// return nil, nil
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &committed)

	go s.sendToAllFollowersInParallel()

	success := <-committed
	// check if entry sent to all followers
	if success {
		s.lastApplied = s.commitIndex
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, ERR_SERVER_CRASHED
}

func (s *RaftSurfstore) sendToAllFollowersInParallel() {
	// send entry to all my followers and count replies
	targetIdx := s.commitIndex + 1
	pendingIdx := int64(len(s.pendingCommits) - 1)
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if s.id == int64(idx) {
			continue
		}
		go s.sendToFollower(addr, targetIdx, responses)
	}
	totalResponses := 1
	totalAppends := 1
	for {
		result := <-responses
		crashed := s.checkCrashed()
		if crashed {
			*s.pendingCommits[pendingIdx] <- false
			break
		}
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}
	if totalAppends > len(s.peers)/2 {
		*s.pendingCommits[pendingIdx] <- true
		s.commitIndex = targetIdx
	}
}

func (s *RaftSurfstore) sendToFollower(addr string, entryIdx int64, commitChannel chan bool) {
	for {
		crashed := s.checkCrashed()
		if crashed {
			commitChannel <- false
		}
		// addr := s.peers[id]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("addr %s entryIdx %d error in sendToFollower\n", addr, entryIdx)
			log.Println(err)
			return
		}
		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64
		if entryIdx == 0 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[entryIdx-1].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: entryIdx - 1,
			PrevLogTerm:  prevLogTerm,
			Entries:      s.log[:entryIdx+1],
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output != nil {
			if output.Success {
				commitChannel <- true
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
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	crashed := s.checkCrashed()
	if crashed {
		return &AppendEntryOutput{Success: false, MatchedIndex: -1}, ERR_SERVER_CRASHED
	}
	// leader := s.checkLeader()
	// if !leader {
	// 	return nil, ERR_NOT_LEADER
	// }

	term := input.Term
	if term < s.term {
		return &AppendEntryOutput{Success: false, MatchedIndex: -1}, nil
	}
	if term > s.term {
		s.isLeaderMutex.RLock()
		s.isLeader = false
		s.isLeaderMutex.RUnlock()
		s.term = term
	}

	for idx, entry := range s.log {
		s.lastApplied = int64(idx - 1)
		if len(input.Entries) < idx+1 {
			s.log = s.log[:idx] // get first idx entries
			input.Entries = make([]*UpdateOperation, 0)
			break
		}
		if entry != input.Entries[idx] {
			s.log = s.log[:idx]
			input.Entries = input.Entries[idx:]
			break
		}
		if len(s.log) == idx+1 {
			input.Entries = input.Entries[idx+1:]
		}
	}

	s.log = append(s.log, input.Entries...)

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}

	return &AppendEntryOutput{Success: true, MatchedIndex: -1}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	crashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if !crashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	log.Println("Previous term - ", s.term)
	s.term = s.term + 1
	log.Println("New term - ", s.term)
	s.isLeaderMutex.RLock()
	s.isLeader = true
	s.isCrashedMutex.RUnlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	crashed := s.checkCrashed()
	if crashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	leader := s.checkLeader()
	if !leader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	fmt.Println("Not crashed and alive")
	live := false
	up := 1
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Println("Error while dialing server: ", err)
			return &Success{Flag: false}, err
		}
		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64
		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[s.commitIndex].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: s.commitIndex,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, input)
		if output != nil {
			up++
			if up > len(s.peers)/2 {
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
