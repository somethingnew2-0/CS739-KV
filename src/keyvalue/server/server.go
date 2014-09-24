package server

import (
	"keyvalue/protobuf"

	"code.google.com/p/goprotobuf/proto"

	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const MaxSetsPerSec uint = 1024

type set struct {
	Key   string
	Value string
}

type Server struct {
	Port           uint16
	listener       net.Listener
	store          map[string]string
	storeLock      *sync.RWMutex // Maps aren't thread safe, must lock on writes using a readers-writer lock
	pending        chan *set     // Pending sets are sent to channel to be added
	pendingPersist chan *set
}

func Init(port uint16) (int, *Server) {
	//Listen to the TCP port
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Printf("Port %u could not be opened: %v\n", port, err)
		return -1, nil
	}

	server := &Server{
		Port:           port,
		listener:       listener,
		store:          make(map[string]string),
		storeLock:      &sync.RWMutex{},
		pending:        make(chan *set, 64),
		pendingPersist: make(chan *set, MaxSetsPerSec),
	}

	go server.run()
	go server.set()
	go server.persist()

	return 0, server
}

func (s *Server) run() {
	for {
		if conn, err := s.listener.Accept(); err == nil {
			go func(s *Server, conn net.Conn) {
				defer conn.Close()
				log.Println("Connection established")
				for {
					data := make([]byte, 4)
					_, err := conn.Read(data)
					if err != nil {
						log.Printf("Error reading length: %v", err)
						return
					}
					length64, _ := binary.Uvarint(data)
					length := int(length64)

					data = make([]byte, length)
					for i := 0; i < length; {
						//Read the data waiting on the connection and put it in the data buffer
						n, err := conn.Read(data[i : length-i])
						i += n
						if err != nil {
							log.Printf("Error reading request: %v", err)
							return
						}
					}
					//Create an struct pointer of type protobuf.Request and protobuf.Response struct
					request := new(protobuf.Request)
					//Convert all the data retrieved into the ProtobufTest.TestMessage struct type
					err = proto.Unmarshal(data[:length], request)
					if err != nil {
						log.Printf("Error in Unmarshalling: %v\n", err)
						return
					}
					response := new(protobuf.Response)
					response.Id = request.Id
					if request.GetValue() == "" {
						result, value := s.Get(request.GetKey())
						response.Result = proto.Int32(int32(result))
						response.Value = proto.String(value)
					} else {
						result, value := s.Set(request.GetKey(), request.GetValue())
						response.Result = proto.Int32(int32(result))
						response.Value = proto.String(value)
					}

					data, err = proto.Marshal(response)
					if err != nil {
						log.Printf("Marshaling error: %v\n", err)
						continue
					}

					length = len(data)
					lengthBytes := make([]byte, 4)
					binary.LittleEndian.PutUint32(lengthBytes, uint32(length))
					_, err = conn.Write(lengthBytes)
					if err != nil {
						log.Printf("Error writing data: %v\n", err)
						return
					}
					_, err = conn.Write(data)
					if err != nil {
						log.Printf("Error writing data: %v\n", err)
						return
					}
				}
			}(s, conn)
		} else {
			continue
		}
	}
}

func (s *Server) set() {
	for set := range s.pending {
		s.storeLock.Lock()
		s.store[set.Key] = set.Value
		s.storeLock.Unlock()

		s.pendingPersist <- set
	}
}

func (s *Server) persist() {
	ticker := time.NewTicker(time.Millisecond * 1000)
	for t := range ticker.C {
		func(s *Server) {
			length := len(s.pendingPersist)
			if length == 0 {
				return
			}

			buffer := make([]*set, length)
			for i := 0; i < length; i++ {
				buffer[i] = <-s.pendingPersist
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

			data, err := json.Marshal(buffer)
			if err != nil {
				log.Printf("Could not marshall delta log, with error: %v\n", err)
			}
			w.Write(data)
			if err != nil {
				log.Printf("Could not write data failed, with error: %v\n", err)
			}
		}(s)
	}
}

func (s *Server) Get(key string) (int, string) {
	if s.store == nil {
		log.Printf("Server Store is not initialized\n")
		return -1, ""
	}

	s.storeLock.RLock()
	value, present := s.store[key]
	s.storeLock.RUnlock()
	if present {
		return 0, value
	}
	return 1, ""
}

func (s *Server) Set(key string, value string) (int, string) {
	status, oldValue := s.Get(key)

	s.pending <- &set{Key: key, Value: value}

	return status, oldValue
}

func (s *Server) Close() {
	s.listener.Close()
}
