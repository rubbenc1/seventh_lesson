.PHONY: install gen test

install:
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

gen: check-env
	protoc --version
	protoc-gen-go --version
	protoc-gen-go-grpc --version
	protoc --go_out=. --go-grpc_out=. service.proto
	# in combination with 'option go_package = ".";' in service.proto this will generate files in this folder with package main
	sed -i "" 's/package __/package main/g' *.pb.go 

test:
	go test -v -race

check-env:
ifndef GOBIN
	$(error GOBIN is undefined, set GOBIN so protoc can see installed plugins in PATH)
endif
