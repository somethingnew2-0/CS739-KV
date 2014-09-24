package main

import (
	"keyvalue/client"
	"keyvalue/server"

	"github.com/jessevdk/go-flags"

	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type service interface {
	Get(key string) (int, string)
	Set(key string, value string) (int, string)
	Close()
}

type operation struct {
	key   string
	value string
}

var (
	opts struct {
		// Callbacks called each time the option is found.
		Get func(string) `short:"g" long:"get" description:"Get a key from the server"`
		Set func(string) `short:"s" long:"set" description:"Set a key on the server (key=value)"`

		// Boolean for whether this should act as a server or client
		Client bool `short:"c" long:"client" description:"Acts as a client when specified"`
	}
	args       []string
	operations = make(chan operation, len(os.Args))
)

func init() {
	opts.Get = func(key string) {
		operations <- operation{key: key}
	}

	opts.Set = func(keyvalue string) {
		split := strings.Split(keyvalue, "=")
		if len(split) < 2 {
			log.Fatalf("Set operation '-s %s' must be in the form '-s key=value'\n", keyvalue)
		}
		operations <- operation{key: split[0], value: strings.Join(split[1:], "=")}
	}

	var err error
	args, err = flags.Parse(&opts)
	if err != nil {
		log.Fatalf("Error parsing options: &v\n", err)
	}

	if len(args) < 1 {
		if opts.Client {
			log.Fatalln("Need to specify address for client to connect")
		} else {
			log.Fatalln("Need to specify port for server to open")
		}
	}

	if opts.Client {
		close(operations)
	}

	// Set runtime GOMAXPROCS
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var service service
	if opts.Client {
		_, service = client.Init(args[0])
	} else {
		port, err := strconv.Atoi(args[0])
		if err == nil {
			_, service = server.Init(uint16(port))
		} else {
			split := strings.Split(args[0], ":")
			port, err := strconv.Atoi(split[len(split)-1])
			if err == nil {
				_, service = server.Init(uint16(port))
			} else {
				log.Fatalf("Could not parse port from '%s': %v", args[0], err)
			}
		}
	}

	defer service.Close()

	for oper := range operations {
		if oper.value == "" {
			result, value := service.Get(oper.key)
			log.Printf("Called Get(key=%s) Received(result=%d, value=%s)\n", oper.key, result, value)
		} else {
			result, old := service.Set(oper.key, oper.value)
			log.Printf("Called Set(key=%s, value=%s) Received(result=%d, value=%s)\n", oper.key, oper.value, result, old)
		}
	}
}
