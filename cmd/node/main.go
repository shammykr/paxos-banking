package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"paxosbank/internal/config"
	"paxosbank/internal/node"
	pb "paxosbank/proto/paxospb"
)

func main() {
	id := flag.Uint("id", 0, "node id (must exist in config)")
	cfgPath := flag.String("config", "config/nodes.yaml", "path to cluster config")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	n, cluster, err := cfg.FindNode(uint32(*id))
	if err != nil {
		log.Fatalf("%v", err)
	}

	srv := node.NewServer(uint32(*id), cfg)

	lis, err := net.Listen("tcp", n.Addr)
	if err != nil {
		log.Fatalf("listen %s: %v", n.Addr, err)
	}

	g := grpc.NewServer()
	pb.RegisterPaxosNodeServer(g, srv)

	log.Printf("node %d up on %s (cluster %d, items %d-%d)",
		*id, n.Addr, cluster.ID, cluster.ShardFrom, cluster.ShardTo)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
