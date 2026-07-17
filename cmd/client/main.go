// Interactive client. Commands:
//   send <sender> <receiver> <amount>
//   balance <itemID>
//   quit
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"paxosbank/internal/config"
	pb "paxosbank/proto/paxospb"
)

func main() {
	cfgPath := flag.String("config", "config/nodes.yaml", "path to cluster config")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// For now: talk to the first node of cluster 1.
	// TODO: discover the leader dynamically; later, route by shard.
	addr := cfg.Clusters[0].Nodes[0].Addr
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	c := pb.NewPaxosNodeClient(conn)

	fmt.Println("commands: send <s> <r> <amt> | balance <id> | quit")
	sc := bufio.NewScanner(os.Stdin)
	for fmt.Print("> "); sc.Scan(); fmt.Print("> ") {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		switch fields[0] {
		case "quit", "exit":
			cancel()
			return
		case "send":
			if len(fields) != 4 {
				fmt.Println("usage: send <sender> <receiver> <amount>")
				break
			}
			s, _ := strconv.ParseUint(fields[1], 10, 64)
			r, _ := strconv.ParseUint(fields[2], 10, 64)
			amt, _ := strconv.ParseInt(fields[3], 10, 64)
			reply, err := c.Submit(ctx, &pb.ClientRequest{Txn: &pb.Transaction{Sender: s, Receiver: r, Amount: amt}})
			if err != nil {
				fmt.Println("rpc error:", err)
			} else if !reply.Ok {
				fmt.Println("rejected:", reply.Error)
			} else {
				fmt.Println("ok")
			}
		case "balance":
			if len(fields) != 2 {
				fmt.Println("usage: balance <itemID>")
				break
			}
			id, _ := strconv.ParseUint(fields[1], 10, 64)
			reply, err := c.GetBalance(ctx, &pb.BalanceRequest{ItemId: id})
			if err != nil {
				fmt.Println("rpc error:", err)
			} else {
				fmt.Println(reply.Balance)
			}
		default:
			fmt.Println("unknown command:", fields[0])
		}
		cancel()
	}
}
