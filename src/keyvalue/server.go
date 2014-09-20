package keyvalue

import (
	"log"
	"os"
	"strconv"
	"strings"
)

const MaxUInt16 uint = uint(^uint16(0))

type Write struct {
	Key   string
	Value string
}

type Server struct {
	Host     string
	Port     uint16
	Store    map[string]string
	Write    chan Write
	Log      chan Write
	DeltaLog *os.File
	BaseLog  *os.File
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
		Host:     split[0],
		Port:     uint16(port),
		Store:    make(map[string]string),
		Write:    make(chan Write, 64),
		DeltaLog: nil,
		BaseLog:  nil,
	}

	go server.write()

	return 0, server
}

func (s *Server) write() {
	for write := range s.Write {
		s.Store[write.Key] = s.Store[write.Value]
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
