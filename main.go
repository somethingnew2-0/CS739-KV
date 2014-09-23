package main

import (
	"keyvalue/client"
	"keyvalue/server"

	"fmt"
)

func main() {
	server.Init(12345)
	_, client := client.Init("localhost:12345")

	result, old := client.Set("key1", "value1")

	fmt.Printf("Called Set(key=%s, value=%s) Received(result=%d, value=%s)\n", "key1", "value1", result, old)

	result, value := client.Get("key1")

	fmt.Printf("Called Get(key=%s) Received(result=%d, value=%s)\n", "key1", result, value)

}
