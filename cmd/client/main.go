// Command raft-kv-client performs a single client operation against a running
// cluster: put, get, delete, or status. It reads the same JSON config as the
// servers, so it knows every member's ID and client address.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/neural-agi/raft-kv/client"
	"github.com/neural-agi/raft-kv/cluster"
	"github.com/neural-agi/raft-kv/raft"
)

const opTimeout = 20 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr *os.File) int {
	flags := flag.NewFlagSet("raft-kv-client", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to a node's JSON cluster config")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "usage: raft-kv-client -config <path> <put key value | get key | delete key | status>")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "raft-kv-client: -config is required")
		flags.Usage()
		return 1
	}
	rest := flags.Args()
	if len(rest) == 0 {
		fmt.Fprintln(stderr, "raft-kv-client: an operation is required")
		flags.Usage()
		return 1
	}

	cfg, err := cluster.LoadProcessConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "raft-kv-client: %v\n", err)
		return 1
	}
	c := client.NewClient(cfg.ClientAddresses())
	c.SetTimeouts(3*time.Second, 5*time.Second, opTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	switch rest[0] {
	case "put":
		if len(rest) != 3 {
			fmt.Fprintln(stderr, "raft-kv-client: usage: put <key> <value>")
			return 1
		}
		if err := c.Put(ctx, []byte(rest[1]), []byte(rest[2])); err != nil {
			fmt.Fprintf(stderr, "raft-kv-client: put: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "ok %s\n", rest[1])

	case "get":
		if len(rest) != 2 {
			fmt.Fprintln(stderr, "raft-kv-client: usage: get <key>")
			return 1
		}
		value, found, err := c.Get(ctx, []byte(rest[1]))
		if err != nil {
			fmt.Fprintf(stderr, "raft-kv-client: get: %v\n", err)
			return 1
		}
		if !found {
			fmt.Fprintln(stdout, "missing")
			return 0
		}
		fmt.Fprintf(stdout, "%s\n", value)

	case "delete":
		if len(rest) != 2 {
			fmt.Fprintln(stderr, "raft-kv-client: usage: delete <key>")
			return 1
		}
		if err := c.Delete(ctx, []byte(rest[1])); err != nil {
			fmt.Fprintf(stderr, "raft-kv-client: delete: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "deleted %s\n", rest[1])

	case "status":
		printed := 0
		for _, id := range cfg.NodeIDs() {
			role, leader, err := c.Status(ctx, id)
			if err != nil {
				continue
			}
			fmt.Fprintf(stdout, "%s role=%s leader=%s\n", id, roleName(role), leaderName(leader))
			printed++
		}
		if printed == 0 {
			fmt.Fprintln(stderr, "raft-kv-client: no reachable member")
			return 1
		}

	default:
		fmt.Fprintf(stderr, "raft-kv-client: unknown operation %q\n", rest[0])
		flags.Usage()
		return 1
	}
	return 0
}

func roleName(role raft.Role) string { return role.String() }

func leaderName(leader raft.NodeID) string {
	if leader == "" {
		return "none"
	}
	return string(leader)
}
