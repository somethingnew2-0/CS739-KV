CS739-KV
========

Key Value Store Mini-Project

The goal for this project is to implement a small distributed system, to get experience in cooperatively developing a protocol specification, and to get experience benchmarking a system.

The service to provide is a simple key-value store, where keys and values are strings. You service should be as consistent as possible, so that requesting a value should return the most recently value set as often as possible. Furthermore, your service should recover from failures, so you will need to store data persistently.

## Run

### Server

The main executable can be run as either the server or the client.  

To execute as the server only the port number is needed as an argument.
```
go run main.go 12345
```

To reset the persistence state, so that the server is reset and booted.  Can also be done manually with `rm -r log/`
```
go run main.go -r 12345
```

### Client

To execute as the client and interact with the server the -c or --client flag is needed along with the server address.
```
go run main.go -c localhost:12345
```

To do something actually interesting specifiy the -g or --get flag to grab values from the server or -s or --set flag to set values in the form `-s key=value`. Simply done
```
go run main.go -c -s key=value -g key localhost:12345
```

What's magical about this command line tool is you can specify mulitple get and set flags in the same client command and they will be executed in order.  For example try this magic 
```
go run main.go -c -s key=value -g key -s key2=value2 -g key -g key2 localhost:12345
```


## Development
Run `source install.sh` or more simply `. install.sh` to setup the git hooks and GOPATH for this new project.
