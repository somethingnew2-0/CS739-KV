package main

import (
	"keyvalue/client"
	"keyvalue/server"

	"fmt"
)

func main() {
	fmt.Printf("Hello, world.\n")
	_, server := server.Init("localhost:12345")
	fmt.Printf("Found server %s on port %d\n", server.Host, server.Port)

	client.ClientTest()
}
