package client

import (
	"errors"

	"code.google.com/p/goprotobuf/proto"

	"./service_pb"
)

func clientTest() {
	stub, client, err := kvservice.DialKVService("tcp", "localhost:12345")
	if err != nil {
		log.Fatal(`kvservice.DialKVService("tcp", "localhost:12345"):`, err)
	}
	defer client.Close()

	var getArg GetRequest
	var setArg SetRequest
	var reply Response

	setArg.key = proto.string("key1")
	setArg.value = proto.string("value1")

	if err = stub.Set(&setArg, &reply); err != nil {
		log.Fatal("kvservice error:", err)
	}

	fmt.Printf("Called Set(key=%s, value=%s) Received(result=%d, value=%s)", setArg.GetKey(), setArg.GetValue(), reply.GetResult(), reply.GetValue())

	getArg.key = proto.string("key1")

	if err = stub.Get(&getArg, &reply); err != nil {
		log.Fatal("kvservice error:", err)
	}

	fmt.Printf("Called Get(key=%s) Received(result=%d, value=%s)", getArg.GetKey(), reply.GetResult(), reply.GetValue())
}
