# Makefile: developer targets for vdb-mysql-engine.
GO_IMAGE ?= golang:1.27.4
PROTOC_GEN_GO ?= v1.36.12

.PHONY: proto test lint

# proto regenerates protocol/vdb.pb.go from protocol/vdb.proto in a container,
# so no local protoc is needed. CI fails if the result differs (doc 07 §5).
proto:
	docker run --rm -v "$(CURDIR)":/src -w /src $(GO_IMAGE) sh /src/protocol/gen.sh $(PROTOC_GEN_GO)

test:
	go test -race ./...

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

