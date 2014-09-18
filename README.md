CS739-KV
========

Key Value Store Mini-Project

The goal for this project is to implement a small distributed system, to get experience in cooperatively developing a protocol specification, and to get experience benchmarking a system.

The service to provide is a simple key-value store, where keys and values are strings. You service should be as consistent as possible, so that requesting a value should return the most recently value set as often as possible. Furthermore, your service should recover from failures, so you will need to store data persistently.

## Development
Run `source install.sh` or more simply `. install.sh` to setup the git hooks and GOPATH for this new project.
