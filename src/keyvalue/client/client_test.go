package client

import (
	"keyvalue/server"
	"testing"
)

func TestClientInit(t *testing.T) {
	server.Init(12345)
	status, client := Init("localhost:12345")

	if status != 0 {
		t.Fatal("Client inited with nonzero status")
	}
	if client == nil {
		t.Fatal("Client inited returned nil value")
	}
}
