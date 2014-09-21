package server

import (
	"keyvalue/protobuf"

	"github.com/eapache/channels"

	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const MaxUInt16 uint = uint(^uint16(0))

type Write struct {
	Key   string
	Value string
}

type Server struct {
	Host  string
	Port  uint16
	Store map[string]string
	Write chan Write
	Log   *channels.InfiniteChannel
}

func Init(host string) (int, *Server) {
	split := strings.Split(host, ":")
	if len(split) != 2 {
		log.Printf("Server given '%s' must be in format 'host:port'\n", host)
		return -1, nil
	}

	port, err := strconv.Atoi(split[1])
	if err != nil {
		log.Printf("Port given '%s' is not a number\n", split[1])
		return -1, nil
	}

	if uint(port) > MaxUInt16 {
		log.Printf("Port given '%u' is too large\n", split[1])
		return -1, nil
	}

	server := &Server{
		Host:  split[0],
		Port:  uint16(port),
		Store: make(map[string]string),
		Write: make(chan Write, 64),
		Log:   channels.NewInfiniteChannel(),
	}

	go server.write()
	go server.log()
	go server.run()

	return 0, server
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
				log.Printf("Could not create file %s, failed with error '%v'\n", deltaPath, err)
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
						log.Printf("Could not marshall delta log, with error '%v'\n", err)
					}
					w.Write(data)
					if err != nil {
						log.Printf("Could not write data failed, with error '%v'\n", err)
					}
					w.WriteString("\n")
					if err != nil {
						log.Printf("Could not write newline, failed with error '%v'\n", err)
					}
					bufferIndex = 0
				}
				bufferIndex++
			}
		}(s)
	}
}

func (s *Server) run() {
	kvservice.ListenAndServeKVService("tcp", fmt.Sprintf(":%d", s.Port), &KVService{Server: s})
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
