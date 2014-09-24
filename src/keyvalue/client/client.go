package client

import (
	"keyvalue/protobuf"

	"crypto/sha256"
	"encoding/binary"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"

	"code.google.com/p/goprotobuf/proto"
)

const MaxUInt16 uint = uint(^uint16(0))

type Client struct {
	Host     string
	Port     uint16
	conn     net.Conn
	connLock sync.Mutex // Don't let multiple go routines write to the connection at once
	pending  map[string]chan protobuf.Response
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
		log.Printf("Port given '%s' is not a number: %v\n", split[1], err)
		return -1, nil
	}

	if uint(port) > MaxUInt16 {
		log.Printf("Port given '%u' is too large\n", split[1])
		return -1, nil
	}

	conn, err := net.Dial("tcp", server)
	if err != nil {
		log.Printf("Cannot connect to '%s' server: %v\n", server, err)
		return -1, nil
	}

	client := &Client{
		Host:     host,
		Port:     uint16(port),
		conn:     conn,
		connLock: sync.Mutex{},
		pending:  make(map[string]chan protobuf.Response),
	}

	go client.run()

	return 0, client
}

func (c *Client) run() {
	for {
		data := make([]byte, 4)
		_, err := c.conn.Read(data)
		if err != nil {
			log.Printf("Error reading length: %v", err)
		}
		length64, _ := binary.Uvarint(data)
		length := int(length64)
		data = make([]byte, length)
		for i := 0; i < length; {
			//Read the data waiting on the connection and put it in the data buffer
			n, err := c.conn.Read(data[i : length-i])
			i += n
			if err != nil {
				log.Printf("Error reading request: %v", err)
				break
			}
		}

		response := new(protobuf.Response)
		err = proto.Unmarshal(data, response)
		if err != nil {
			log.Fatal("Unmarshaling error: ", err)
		}
		callback := c.pending[response.GetId()]
		callback <- *response
		close(callback)
		delete(c.pending, response.GetId())
	}
}

// No entropy added with hashing here, could just send random int instead
func randomId() string {
	random := rand.Uint32()
	randomBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(randomBytes, random)
	hash := sha256.Sum256(randomBytes)
	return string(hash[:])
}

func (c *Client) write(request *protobuf.Request) chan protobuf.Response {
	data, err := proto.Marshal(request)
	if err != nil {
		log.Printf("Marshaling error: %v\n", err)
		return nil
	}

	length := len(data)
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(length))

	// Guarantee squential write of length then protobuf on stream
	c.connLock.Lock()
	defer c.connLock.Unlock()
	_, err = c.conn.Write(lengthBytes)
	if err != nil {
		log.Printf("Error writing data: %v\n", err)
		return nil
	}
	_, err = c.conn.Write(data)
	if err != nil {
		log.Printf("Error writing data: %v\n", err)
		return nil
	}

	callback := make(chan protobuf.Response)
	c.pending[request.GetId()] = callback
	return callback
}

func (c *Client) Get(key string) (int, string) {
	request := new(protobuf.Request)
	request.Id = proto.String(randomId())
	request.Key = proto.String(key)

	callback := c.write(request)
	if callback == nil {
		return -1, ""
	}

	// Block on callback
	response := <-callback
	return int(response.GetResult()), response.GetValue()
}

func (c *Client) Set(key string, value string) (int, string) {
	request := new(protobuf.Request)
	request.Id = proto.String(randomId())
	request.Key = proto.String(key)
	request.Value = proto.String(value)

	callback := c.write(request)
	if callback == nil {
		return -1, ""
	}

	// Block on callback
	response := <-callback
	return int(response.GetResult()), response.GetValue()
}

func (c *Client) Close() {
	c.conn.Close()
}
