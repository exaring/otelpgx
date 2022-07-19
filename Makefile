GO ?= go

all: test

.PHONY: test
test: vet

.PHONY: lint
lint:
	golint ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: deps
deps:
	go get -u golang.org/x/lint/golint