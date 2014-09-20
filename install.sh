#!/bin/bash
export GOPATH=$PWD

go get -d

chmod +x hooks/pre-commit
ln -s ../../hooks/pre-commit .git/hooks/
