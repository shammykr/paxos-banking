// Package paxos contains the pure consensus logic, deliberately free of any
// networking code. This makes it unit-testable without gRPC: tests wire
// proposers and acceptors together with an in-memory Transport.
package paxos

// Ballot is a proposal number: ordered by Round, with NodeID as tiebreaker.
// Because NodeID is unique per node, no two nodes ever produce equal ballots.
type Ballot struct {
	Round  uint64
	NodeID uint32
}

// Less reports whether b < o.
func (b Ballot) Less(o Ballot) bool {
	if b.Round != o.Round {
		return b.Round < o.Round
	}
	return b.NodeID < o.NodeID
}

// GreaterEqual reports whether b >= o.
func (b Ballot) GreaterEqual(o Ballot) bool {
	return !b.Less(o)
}

// Txn is a banking transaction (the "value" our Paxos instances agree on).
type Txn struct {
	Sender   uint64
	Receiver uint64
	Amount   int64
}

// AcceptedEntry records that some value was accepted at a log slot under a ballot.
type AcceptedEntry struct {
	Slot   uint64
	Ballot Ballot
	Value  Txn
}

// ---- Phase 1 messages ----

type PrepareReq struct {
	Ballot   Ballot
	FromSlot uint64
}

type PromiseResp struct {
	OK       bool
	Promised Ballot          // acceptor's current promise (set on rejection too)
	Accepted []AcceptedEntry // entries at/after FromSlot the acceptor already accepted
}

// ---- Phase 2 messages ----

type AcceptReq struct {
	Ballot Ballot
	Slot   uint64
	Value  Txn
}

type AcceptedResp struct {
	OK       bool
	Promised Ballot
}

// Transport abstracts how a proposer talks to acceptors. In production this
// is gRPC (internal/node); in tests it's an in-memory fake.
type Transport interface {
	Prepare(peerID uint32, req PrepareReq) (PromiseResp, error)
	Accept(peerID uint32, req AcceptReq) (AcceptedResp, error)
}
