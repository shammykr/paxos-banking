package paxos

// Tests for the pure consensus core. Run: go test -race ./internal/paxos/ -v

import (
	"sync"
	"testing"
)

// ---------- in-memory transport wiring proposers to acceptors ----------

type localTransport struct {
	mu        sync.Mutex
	acceptors map[uint32]*Acceptor
	down      map[uint32]bool // simulate crashed/partitioned nodes
}

func newLocalTransport(ids ...uint32) *localTransport {
	t := &localTransport{acceptors: map[uint32]*Acceptor{}, down: map[uint32]bool{}}
	for _, id := range ids {
		t.acceptors[id] = NewAcceptor()
	}
	return t
}

type errDown struct{}

func (errDown) Error() string { return "node down" }

func (t *localTransport) Prepare(peer uint32, req PrepareReq) (PromiseResp, error) {
	t.mu.Lock()
	isDown := t.down[peer]
	a := t.acceptors[peer]
	t.mu.Unlock()
	if isDown {
		return PromiseResp{}, errDown{}
	}
	return a.HandlePrepare(req), nil
}

func (t *localTransport) Accept(peer uint32, req AcceptReq) (AcceptedResp, error) {
	t.mu.Lock()
	isDown := t.down[peer]
	a := t.acceptors[peer]
	t.mu.Unlock()
	if isDown {
		return AcceptedResp{}, errDown{}
	}
	return a.HandleAccept(req), nil
}

func (t *localTransport) setDown(peer uint32, down bool) {
	t.mu.Lock()
	t.down[peer] = down
	t.mu.Unlock()
}

// ---------- acceptor unit tests ----------

func TestAcceptorPromisesHigherBallot(t *testing.T) {
	a := NewAcceptor()

	r1 := a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 1, NodeID: 1}})
	if !r1.OK {
		t.Fatal("acceptor should promise the first ballot it sees")
	}

	r2 := a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 2, NodeID: 1}})
	if !r2.OK {
		t.Fatal("acceptor should promise a higher ballot")
	}
}

func TestAcceptorRejectsLowerBallot(t *testing.T) {
	a := NewAcceptor()

	a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 5, NodeID: 2}})

	r := a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 3, NodeID: 1}})
	if r.OK {
		t.Fatal("acceptor must reject a Prepare with a lower ballot than promised")
	}
	if r.Promised != (Ballot{Round: 5, NodeID: 2}) {
		t.Fatalf("rejection must report the promised ballot, got %+v", r.Promised)
	}

	// Equal round, lower node id is also a lower ballot.
	r = a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 5, NodeID: 1}})
	if r.OK {
		t.Fatal("ballot (5,1) < (5,2): must be rejected")
	}
}

func TestAcceptorRejectsStaleAccept(t *testing.T) {
	a := NewAcceptor()

	a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 9, NodeID: 3}})

	r := a.HandleAccept(AcceptReq{Ballot: Ballot{Round: 4, NodeID: 1}, Slot: 0, Value: Txn{Sender: 1, Receiver: 2, Amount: 1}})
	if r.OK {
		t.Fatal("acceptor must reject an Accept with a ballot lower than its promise")
	}
}

func TestAcceptorAcceptWithoutPriorPrepareIsOK(t *testing.T) {
	// An acceptor that never saw Phase 1 must still accept (promised is zero-value).
	a := NewAcceptor()
	r := a.HandleAccept(AcceptReq{Ballot: Ballot{Round: 1, NodeID: 1}, Slot: 0, Value: Txn{Sender: 1, Receiver: 2, Amount: 1}})
	if !r.OK {
		t.Fatal("acceptor with no prior promise must accept")
	}
}

func TestPromiseReturnsPreviouslyAccepted(t *testing.T) {
	a := NewAcceptor()

	v := Txn{Sender: 7, Receiver: 8, Amount: 100}
	a.HandleAccept(AcceptReq{Ballot: Ballot{Round: 1, NodeID: 1}, Slot: 3, Value: v})

	r := a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 2, NodeID: 2}, FromSlot: 0})
	if !r.OK {
		t.Fatal("higher ballot should be promised")
	}
	if len(r.Accepted) != 1 || r.Accepted[0].Slot != 3 || r.Accepted[0].Value != v {
		t.Fatalf("promise must carry previously accepted entries, got %+v", r.Accepted)
	}

	// Entries below FromSlot must be filtered out.
	r = a.HandlePrepare(PrepareReq{Ballot: Ballot{Round: 3, NodeID: 2}, FromSlot: 4})
	if len(r.Accepted) != 0 {
		t.Fatalf("entries below FromSlot must not be returned, got %+v", r.Accepted)
	}
}

// ---------- proposer integration tests ----------

func TestSingleProposerChoosesItsOwnValue(t *testing.T) {
	tr := newLocalTransport(1, 2, 3)
	p := NewProposer(1, []uint32{1, 2, 3}, tr)

	v := Txn{Sender: 1, Receiver: 2, Amount: 5}
	chosen, err := p.Propose(0, v)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	if chosen != v {
		t.Fatalf("with no competition, proposer's own value must be chosen; got %+v", chosen)
	}
}

func TestProposerToleratesOneNodeDown(t *testing.T) {
	tr := newLocalTransport(1, 2, 3)
	tr.setDown(3, true)

	p := NewProposer(1, []uint32{1, 2, 3}, tr)
	if _, err := p.Propose(0, Txn{Sender: 1, Receiver: 2, Amount: 5}); err != nil {
		t.Fatalf("2 of 3 nodes is a quorum; propose must succeed: %v", err)
	}
}

func TestProposerFailsWithoutQuorum(t *testing.T) {
	tr := newLocalTransport(1, 2, 3)
	tr.setDown(2, true)
	tr.setDown(3, true)

	p := NewProposer(1, []uint32{1, 2, 3}, tr)
	if _, err := p.Propose(0, Txn{Sender: 1, Receiver: 2, Amount: 5}); err == nil {
		t.Fatal("1 of 3 nodes is not a quorum; propose must fail")
	}
}

// THE core safety test: once a value is chosen, a later proposer with a
// higher ballot must adopt it, not overwrite it.
func TestCompetingProposersConverge(t *testing.T) {
	tr := newLocalTransport(1, 2, 3)

	p1 := NewProposer(1, []uint32{1, 2, 3}, tr)
	v1 := Txn{Sender: 1, Receiver: 2, Amount: 111}
	chosen1, err := p1.Propose(0, v1)
	if err != nil {
		t.Fatalf("p1 propose failed: %v", err)
	}

	p2 := NewProposer(2, []uint32{1, 2, 3}, tr)
	v2 := Txn{Sender: 3, Receiver: 4, Amount: 222}
	chosen2, err := p2.Propose(0, v2)
	if err != nil {
		t.Fatalf("p2 propose failed: %v", err)
	}

	if chosen1 != chosen2 {
		t.Fatalf("SAFETY VIOLATION: slot 0 chosen as %+v then %+v", chosen1, chosen2)
	}
	if chosen2 != v1 {
		t.Fatalf("p2 must adopt the already-chosen value %+v, got %+v", v1, chosen2)
	}
}

// Same, but concurrent — run with: go test -race ./internal/paxos/
func TestConcurrentProposersAgree(t *testing.T) {
	tr := newLocalTransport(1, 2, 3)

	results := make([]Txn, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := NewProposer(uint32(i+1), []uint32{1, 2, 3}, tr)
			v := Txn{Sender: uint64(i + 1), Receiver: 9, Amount: int64(i+1) * 100}
			chosen, err := p.Propose(0, v)
			if err != nil {
				t.Errorf("proposer %d failed: %v", i+1, err)
				return
			}
			results[i] = chosen
		}(i)
	}
	wg.Wait()

	if results[0] != results[1] {
		t.Fatalf("SAFETY VIOLATION: proposers decided different values: %+v vs %+v", results[0], results[1])
	}
}
