package server

import "testing"

func TestServerInit(t *testing.T) {
	status, server := Init("localhost:12345")
	if status != 0 {
		t.Fatal("Server inited with nonzero status")
	}
	if server == nil {
		t.Fatal("Server inited returned nil value")
	}
}
