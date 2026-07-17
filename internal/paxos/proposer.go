package paxos

import "errors"

// ErrPreempted is returned when a higher ballot beats ours.
var ErrPreempted = errors.New("preempted by higher ballot")

const maxAttempts = 10

// Proposer drives Phase 1 and Phase 2 against a set of acceptors.
type Proposer struct {
	NodeID uint32
	Peers  []uint32 // all acceptor IDs in the cluster, including self
	Tr     Transport

	round uint64 // last round number used
}

func NewProposer(nodeID uint32, peers []uint32, tr Transport) *Proposer {
	return &Proposer{NodeID: nodeID, Peers: peers, Tr: tr}
}

func (p *Proposer) quorum() int { return len(p.Peers)/2 + 1 }

func (p *Proposer) nextBallot() Ballot {
	p.round++
	return Ballot{Round: p.round, NodeID: p.NodeID}
}

// phase1 sends Prepare(ballot, slot) to all peers and collects promises.
// If any promise reveals an already-accepted entry for this slot, the value
// from the highest-ballot such entry must be adopted (found=true).
// Returns ErrPreempted if no quorum of OK responses.
func (p *Proposer) phase1(b Ballot, slot uint64) (adopted Txn, found bool, err error) {
	panic("TODO: implement")
}

// phase2 sends Accept(ballot, slot, v) to all peers.
// Returns nil on a quorum of OKs, ErrPreempted otherwise.
func (p *Proposer) phase2(b Ballot, slot uint64, v Txn) error {
	panic("TODO: implement")
}

// Propose runs single-decree Paxos for one log slot and returns the value
// actually chosen — which may differ from v if phase1 adopted an earlier
// value. Retries with a fresh ballot on preemption, up to maxAttempts.
func (p *Proposer) Propose(slot uint64, v Txn) (Txn, error) {
	panic("TODO: implement")
}
