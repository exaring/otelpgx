GO ?= go

all: test

.PHONY: test
test: vet
	$(GO) test -v -short ./...

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: vet
vet:
	$(GO) vet ./...