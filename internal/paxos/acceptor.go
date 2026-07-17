package paxos

import "sync"

// Acceptor holds the durable state of one Paxos acceptor.
type Acceptor struct {
	mu       sync.Mutex
	promised Ballot                   // highest ballot this acceptor has promised
	accepted map[uint64]AcceptedEntry // slot -> highest-ballot entry accepted at that slot
}

func NewAcceptor() *Acceptor {
	return &Acceptor{accepted: make(map[uint64]AcceptedEntry)}
}

// HandlePrepare processes a Phase 1 request.
//
// Rules (Paxos Made Simple, §2.2):
//   - If req.Ballot >= promised: record the new promise and reply OK,
//     including every accepted entry with Slot >= req.FromSlot so a new
//     leader can adopt values that may already be chosen.
//   - Otherwise: reply not-OK with the current promised ballot.
func (a *Acceptor) HandlePrepare(req PrepareReq) PromiseResp {
	panic("TODO: implement")
}

// HandleAccept processes a Phase 2 request.
//
// Rules:
//   - If req.Ballot >= promised: update the promise, record the entry,
//     reply OK. (An acceptor that never saw the Prepare must still accept.)
//   - Otherwise: reply not-OK with the current promise.
func (a *Acceptor) HandleAccept(req AcceptReq) AcceptedResp {
	panic("TODO: implement")
}

// Promised returns the current promised ballot.
func (a *Acceptor) Promised() Ballot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.promised
}
