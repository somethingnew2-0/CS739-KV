package main

import (
	"keyvalue"

	"github.com/chai2010/protorpc"

	"fmt"
)

func main() {
	fmt.Printf("Hello, world.\n")
	_, server := keyvalue.Init("localhost:12345")
	fmt.Printf("Found server %s on port %d\n", server.Host, server.Port)
}
