package client

import (
	"keyvalue/protobuf"

	"fmt"
	"log"
	"strconv"
	"strings"

	"code.google.com/p/goprotobuf/proto"
)

const MaxUInt16 uint = uint(^uint16(0))

type Client struct {
	Host string
	Port uint16
}

func Init(server string) (int, *Client) {
	split := strings.Split(server, ":")
	if len(split) != 2 {
		log.Printf("Server given '%s' must be in format 'host:port'\n", server)
		return -1, nil
	}
	host := split[0]

	port, err := strconv.Atoi(split[1])
	if err != nil {
		log.Printf("Port given '%s' is not a number\n", split[1])
		return -1, nil
	}

	if uint(port) > MaxUInt16 {
		log.Printf("Port given '%u' is too large\n", split[1])
		return -1, nil
	}

	client := &Client{
		Host: host,
		Port: uint16(port),
	}
	return 0, client
}

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
