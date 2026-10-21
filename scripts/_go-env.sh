#!/usr/bin/env bash
export PATH=/usr/local/go/bin:/home/parthmalhotra/.local/bin:$PATH
export GOPATH=/home/parthmalhotra/go
export HOME=/home/parthmalhotra
cd /mnt/c/Users/am/Desktop/CC/ReliefGrid
go tool oapi-codegen --help 2>&1 | head -80
