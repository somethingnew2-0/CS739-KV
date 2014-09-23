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

	setArg.Key = proto.String("key1")
	setArg.Value = proto.String("value1")

	if err = stub.Set(setArg, reply); err != nil {
		log.Fatal("kvservice error:", err)
	}

	fmt.Printf("Called Set(key=%s, value=%s) Received(result=%d, value=%s)", setArg.GetKey(), setArg.GetValue(), reply.GetResult(), reply.GetValue())

	getArg.Key = proto.String("key1")

	if err = stub.Get(getArg, reply); err != nil {
		log.Fatal("kvservice error:", err)
	}

	fmt.Printf("Called Get(key=%s) Received(result=%d, value=%s)", getArg.GetKey(), reply.GetResult(), reply.GetValue())

}
