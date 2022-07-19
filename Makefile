GO ?= go

.PHONY: test
test: lint vet

.PHONY: lint
lint:
	golint ./...

.PHONY: vet
vet:
	$(GO) vet ./...



