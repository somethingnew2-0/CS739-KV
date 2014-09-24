package server

import (
	"keyvalue/protobuf"

	"code.google.com/p/goprotobuf/proto"

	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const LogDir string = "log/"
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
	log.Println("Server starting")

	//Listen to the TCP port
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
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

	os.MkdirAll(LogDir, 0777)

	server.recover()
	log.Println("Server fully recovered")

	go server.run()
	go server.set()

	go server.persistDelta()
	go server.persistBase()

	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, "%v", server.store)
		})

		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	log.Println("Server accepting requests")
	return 0, server
}

func (s *Server) recover() {
	entries, err := ioutil.ReadDir(LogDir)
	if err != nil {
		log.Printf("Error reading log directory, unable to recover: %v", err)
		return
	}

	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	sort.Strings(names)

	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	// Find the most recent back backup
	var baseEpoch int64
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		if strings.LastIndex(name, "-base") >= 0 {
			split := strings.Split(name, "-")
			if len(split) == 2 {
				epoch, err := strconv.ParseInt(split[0], 10, 64)
				if err == nil {
					baseEpoch = epoch
				}
			}

			data, err := ioutil.ReadFile(path.Join(LogDir, name))
			if err != nil {
				log.Printf("Error reading base log, unable to recover: %v", err)
				return
			}

			err = json.Unmarshal(data, &s.store)
			if err != nil {
				log.Printf("Error unmarshalling base log, unable to recover: %v", err)
				return
			}

			// Truncate the list of names so we don't have to iterate
			// through all of them for delta recovery
			if len(names) > i+1 {
				names = names[i+1:]
			} else {
				// No further delta updates in the list
				return
			}

			break
		}
	}

	for _, name := range names {
		if strings.LastIndex(name, "-delta") >= 0 {
			split := strings.Split(name, "-")
			if len(split) == 2 {
				epoch, err := strconv.ParseInt(split[0], 10, 64)
				if err == nil && epoch > baseEpoch {
					data, err := ioutil.ReadFile(path.Join(LogDir, fmt.Sprintf("%d-delta", epoch)))
					if err != nil {
						log.Printf("Error reading delta log, recovery could be paritally incorrect: %v", err)
						continue
					}

					var sets []set
					err = json.Unmarshal(data, &sets)
					if err != nil {
						log.Printf("Error reading delta log, recovery could be paritally incorrect: %v", err)
						continue
					}

					for _, set := range sets {
						s.store[set.Key] = set.Value
					}
				}
			}
		}
	}
}

func (s *Server) run() {
	for {
		if conn, err := s.listener.Accept(); err == nil {
			go func(s *Server, conn net.Conn) {
				defer conn.Close()
				log.Println("Connection established with client")
				for {

					data := make([]byte, 4)
					_, err := conn.Read(data)
					if err != nil {
						log.Printf("Error reading length: %v", err)
						return
					}
					length := int(binary.BigEndian.Uint32(data))

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
					if request.GetType() == "get" {
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
					binary.BigEndian.PutUint32(lengthBytes, uint32(length))
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

func (s *Server) persistDelta() {
	ticker := time.NewTicker(time.Second)
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

			deltaPath := path.Join(LogDir, fmt.Sprintf("%d-delta", t.UnixNano()))
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

func (s *Server) persistBase() {
	ticker := time.NewTicker(time.Minute)
	for t := range ticker.C {
		func(s *Server) {
			basePath := path.Join(LogDir, fmt.Sprintf("%d-base", t.UnixNano()))
			f, err := os.Create(basePath)
			if err != nil {
				log.Printf("Could not create file %s, failed with error: %v\n", basePath, err)
				return
			}
			defer f.Close()

			w := bufio.NewWriter(f)
			defer w.Flush()

			s.storeLock.RLock()
			data, err := json.Marshal(s.store)
			s.storeLock.RUnlock()
			if err != nil {
				log.Printf("Could not marshall delta log, with error: %v\n", err)
			}
			w.Write(data)
			if err != nil {
				log.Printf("Could not write data failed, with error: %v\n", err)
			}
			go deleteOldPersistence(t.UnixNano())
		}(s)
	}
}

func deleteOldPersistence(epoch int64) {
	entries, err := ioutil.ReadDir(LogDir)
	if err != nil {
		log.Printf("Error reading log directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.LastIndex(name, "-base") >= 0 || strings.LastIndex(name, "-delta") >= 0 {
			split := strings.Split(name, "-")
			if len(split) == 2 {
				touch, err := strconv.ParseInt(split[0], 10, 64)
				if err == nil && touch < epoch {
					os.Remove(path.Join(LogDir, name))
				}
			}
		}
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
