package server

import (
	"keyvalue/protobuf"

	"code.google.com/p/goprotobuf/proto"
	"github.com/eapache/channels"

	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

type Write struct {
	Key   string
	Value string
}

type Server struct {
	Port     uint16
	Store    map[string]string
	Listener net.Listener
	Write    chan Write
	Log      *channels.InfiniteChannel
}

func Init(port uint16) (int, *Server) {
	//Listen to the TCP port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%u", port))
	if err != nil {
		log.Printf("Port %u could not be opened: %v\n", port, err)
		return -1, nil
	}

	server := &Server{
		Port:     port,
		Store:    make(map[string]string),
		Listener: listener,
		Write:    make(chan Write, 64),
		Log:      channels.NewInfiniteChannel(),
	}

	go server.run()
	go server.write()
	go server.log()

	return 0, server
}

func (s *Server) run() {
	for {
		if conn, err := s.Listener.Accept(); err == nil {
			go func(s *Server, conn net.Conn) {
				log.Println("Connection established")
				//Close the connection when the function exits
				defer conn.Close()
				//Create a data buffer of type byte slice with capacity of 4096
				data := make([]byte, 4096)
				//Read the data waiting on the connection and put it in the data buffer
				n, err := conn.Read(data)
				if err != nil {
					log.Println(err)
				}
				log.Println("Decoding Protobuf message")
				//Create an struct pointer of type protobuf.Request and protobuf.Response struct
				request := new(protobuf.Request)
				response := new(protobuf.Response)
				//Convert all the data retrieved into the ProtobufTest.TestMessage struct type
				err = proto.Unmarshal(data[0:n], request)
				if err != nil {
					log.Println(err)
				}
				if request.Value != nil {
					result, value := s.Get(*request.Key)
					response.Result = proto.Int32(int32(result))
					response.Value = proto.String(value)
				} else {
					result, value := s.Set(*request.Key, *request.Value)
					response.Result = proto.Int32(int32(result))
					response.Value = proto.String(value)
				}
			}(s, conn)
		} else {
			continue
		}
	}
}

func (s *Server) write() {
	for write := range s.Write {
		s.Store[write.Key] = s.Store[write.Value]
		s.Log.In() <- write
	}
}

func (s *Server) log() {
	ticker := time.NewTicker(time.Millisecond * 1000)
	buffer := make([]Write, 1024)
	for t := range ticker.C {
		func(s *Server) {
			length := s.Log.Len()
			if length == 0 {
				return
			}

			deltaPath := fmt.Sprintf("/tmp/delta-%d", t.UnixNano())
			f, err := os.Create(deltaPath)
			if err != nil {
				log.Printf("Could not create file %s, failed with error: %v\n", deltaPath, err)
				return
			}
			defer f.Close()

			w := bufio.NewWriter(f)
			defer w.Flush()

			bufferIndex := 0
			for i := 0; i < length; i++ {
				buffer[bufferIndex] = (<-s.Log.Out()).(Write)
				if bufferIndex > cap(buffer) {
					data, err := json.Marshal(buffer)
					if err != nil {
						log.Printf("Could not marshall delta log, with error: %v\n", err)
					}
					w.Write(data)
					if err != nil {
						log.Printf("Could not write data failed, with error: %v\n", err)
					}
					w.WriteString("\n")
					if err != nil {
						log.Printf("Could not write newline, failed with error: %v\n", err)
					}
					bufferIndex = 0
				}
				bufferIndex++
			}
		}(s)
	}
}

func (s *Server) Get(key string) (int, string) {
	if s.Store == nil {
		log.Printf("Server Store is not initialized\n")
		return -1, ""
	}

	value, present := s.Store[key]
	if present {
		return 0, value
	}
	return 1, ""
}

func (s *Server) Set(key string, value string) (int, string) {
	status, oldValue := s.Get(key)

	if s.Write == nil {
		log.Printf("Server Write channel is not initialized\n")
		return -1, ""
	}

	s.Write <- Write{Key: key, Value: value}

	return status, oldValue
}

func (s *Server) Close() {
	s.Listener.Close()
}
