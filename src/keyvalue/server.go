package keyvalue

import (
	"log"
	"strconv"
	"strings"
)

const MaxUInt16 uint = uint(^uint16(0))

type Server struct {
	Host string
	Port uint16
}

func Init(server string) (int, *Server) {
	split := strings.Split(server, ":")
	if len(split) != 2 {
		log.Printf("Server given '%s' must be in format 'host:port'\n", server)
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

	return 0, &Server{Host: split[0], Port: uint16(port)}
}

func (s *Server) Get(key string) (int, string) {
	return 0, ""
}

func (s *Server) Set(key string, value string) (int, string) {
	return 0, ""
}
