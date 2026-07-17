// Package node is the gRPC glue: it exposes the paxos.Acceptor over the
// network and gives the paxos.Proposer a gRPC-backed Transport.
//
// This file compiles only after you generate protobuf code:
//   ./scripts/gen_proto.sh && go mod tidy
package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"paxosbank/internal/config"
	"paxosbank/internal/paxos"
	pb "paxosbank/proto/paxospb"
)

// ---------- proto <-> internal type conversions ----------

func ballotFromProto(b *pb.Ballot) paxos.Ballot {
	if b == nil {
		return paxos.Ballot{}
	}
	return paxos.Ballot{Round: b.Round, NodeID: b.NodeId}
}

func ballotToProto(b paxos.Ballot) *pb.Ballot {
	return &pb.Ballot{Round: b.Round, NodeId: b.NodeID}
}

func txnFromProto(t *pb.Transaction) paxos.Txn {
	if t == nil {
		return paxos.Txn{}
	}
	return paxos.Txn{Sender: t.Sender, Receiver: t.Receiver, Amount: t.Amount}
}

func txnToProto(t paxos.Txn) *pb.Transaction {
	return &pb.Transaction{Sender: t.Sender, Receiver: t.Receiver, Amount: t.Amount}
}

// ---------- server ----------

// Server is one Paxos node: an acceptor (always) plus, when leader, a proposer.
type Server struct {
	pb.UnimplementedPaxosNodeServer

	ID       uint32
	Cfg      *config.Config
	Acceptor *paxos.Acceptor

	mu    sync.Mutex
	peers map[uint32]pb.PaxosNodeClient // lazily dialed
}

func NewServer(id uint32, cfg *config.Config) *Server {
	return &Server{
		ID:       id,
		Cfg:      cfg,
		Acceptor: paxos.NewAcceptor(),
		peers:    make(map[uint32]pb.PaxosNodeClient),
	}
}

// ---- Paxos RPCs: thin wrappers around the pure logic ----

func (s *Server) Prepare(_ context.Context, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	resp := s.Acceptor.HandlePrepare(paxos.PrepareReq{
		Ballot:   ballotFromProto(req.Ballot),
		FromSlot: req.FromSlot,
	})
	out := &pb.PromiseResponse{Ok: resp.OK, Promised: ballotToProto(resp.Promised)}
	for _, e := range resp.Accepted {
		out.Accepted = append(out.Accepted, &pb.AcceptedEntry{
			Slot:   e.Slot,
			Ballot: ballotToProto(e.Ballot),
			Value:  txnToProto(e.Value),
		})
	}
	return out, nil
}

func (s *Server) Accept(_ context.Context, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	resp := s.Acceptor.HandleAccept(paxos.AcceptReq{
		Ballot: ballotFromProto(req.Ballot),
		Slot:   req.Slot,
		Value:  txnFromProto(req.Value),
	})
	return &pb.AcceptedResponse{Ok: resp.OK, Promised: ballotToProto(resp.Promised)}, nil
}

func (s *Server) Commit(_ context.Context, req *pb.CommitNotice) (*pb.Ack, error) {
	// TODO: apply the committed entry to the state machine, in slot order
	// (buffer out-of-order commits until the gap is filled).
	return &pb.Ack{}, nil
}

func (s *Server) SendHeartbeat(_ context.Context, req *pb.Heartbeat) (*pb.Ack, error) {
	// TODO: reset the election timer; step down if req.LeaderBallot is
	// higher than anything we've seen.
	return &pb.Ack{}, nil
}

func (s *Server) Submit(_ context.Context, req *pb.ClientRequest) (*pb.ClientReply, error) {
	// TODO: if leader, assign next log slot and run Propose; otherwise
	// reply with an error naming the leader.
	return &pb.ClientReply{Ok: false, Error: "not implemented"}, nil
}

func (s *Server) GetBalance(_ context.Context, req *pb.BalanceRequest) (*pb.BalanceReply, error) {
	// TODO: read from the state machine.
	return &pb.BalanceReply{Balance: -1}, nil
}

// ---------- gRPC-backed paxos.Transport ----------

// GRPCTransport lets a Proposer on this node reach acceptors on peers.
type GRPCTransport struct{ S *Server }

const rpcTimeout = 500 * time.Millisecond

func (t *GRPCTransport) client(peer uint32) (pb.PaxosNodeClient, error) {
	t.S.mu.Lock()
	defer t.S.mu.Unlock()
	if c, ok := t.S.peers[peer]; ok {
		return c, nil
	}
	n, _, err := t.S.Cfg.FindNode(peer)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(n.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial node %d: %w", peer, err)
	}
	c := pb.NewPaxosNodeClient(conn)
	t.S.peers[peer] = c
	return c, nil
}

func (t *GRPCTransport) Prepare(peer uint32, req paxos.PrepareReq) (paxos.PromiseResp, error) {
	// Local fast path: don't RPC yourself.
	if peer == t.S.ID {
		return t.S.Acceptor.HandlePrepare(req), nil
	}
	c, err := t.client(peer)
	if err != nil {
		return paxos.PromiseResp{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	r, err := c.Prepare(ctx, &pb.PrepareRequest{Ballot: ballotToProto(req.Ballot), FromSlot: req.FromSlot})
	if err != nil {
		return paxos.PromiseResp{}, err
	}
	out := paxos.PromiseResp{OK: r.Ok, Promised: ballotFromProto(r.Promised)}
	for _, e := range r.Accepted {
		out.Accepted = append(out.Accepted, paxos.AcceptedEntry{
			Slot:   e.Slot,
			Ballot: ballotFromProto(e.Ballot),
			Value:  txnFromProto(e.Value),
		})
	}
	return out, nil
}

func (t *GRPCTransport) Accept(peer uint32, req paxos.AcceptReq) (paxos.AcceptedResp, error) {
	if peer == t.S.ID {
		return t.S.Acceptor.HandleAccept(req), nil
	}
	c, err := t.client(peer)
	if err != nil {
		return paxos.AcceptedResp{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	r, err := c.Accept(ctx, &pb.AcceptRequest{
		Ballot: ballotToProto(req.Ballot),
		Slot:   req.Slot,
		Value:  txnToProto(req.Value),
	})
	if err != nil {
		return paxos.AcceptedResp{}, err
	}
	return paxos.AcceptedResp{OK: r.Ok, Promised: ballotFromProto(r.Promised)}, nil
}
