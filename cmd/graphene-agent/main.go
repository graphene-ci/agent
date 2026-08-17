// Command graphene-agent hosts user code on one Linux machine: it
// connects outbound to the graphene server, reports machine facts, and
// runs per-(machine × run) worker containers. See pkg/host for the core
// types; the connection loop and the runtime implementation land next.
package main

import (
	"fmt"
	"os"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("graphene-agent", version)
		return
	}
	fmt.Fprintln(os.Stderr, "graphene-agent: connection loop not implemented yet; see pkg/host")
	os.Exit(1)
}
