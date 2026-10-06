#!/bin/bash

set -ex

if [ ! -f /usr/local/go/bin/go ]; then
	wget -c https://go.dev/dl/go1.25.13.linux-amd64.tar.gz
	sudo tar -C /usr/local -xzf go1.25.13.linux-amd64.tar.gz
fi

export PATH=/usr/local/go/bin:$PATH

which go
go version

rm -f rkdev_amd64 rkdev_arm64
go mod tidy

BUILD_TIME=$(date '+%Y.%m.%d')

CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -trimpath \
  -ldflags="-s -w -extldflags=-static -X 'main.buildTime=${BUILD_TIME}'" \
  -gcflags="all=-l -B" \
  -o rkdev_arm64 .

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath \
  -ldflags="-s -w -extldflags=-static -X 'main.buildTime=${BUILD_TIME}'" \
  -gcflags="all=-l -B" \
  -o rkdev_amd64 .

ls -al rkdev_amd64 rkdev_arm64
