package client

import (
	"keyvalue/protobuf"

	"fmt"
	"log"

	"code.google.com/p/goprotobuf/proto"
)

func ClientTest() {
	stub, client, err := kvservice.DialKVService("tcp", "localhost:12345")
	if err != nil {
		log.Fatal(`kvservice.DialKVService("tcp", "localhost:12345"):`, err)
	}
	defer client.Close()

	getArg := new(kvservice.GetRequest)
	setArg := new(kvservice.SetRequest)
	reply := new(kvservice.Response)

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
