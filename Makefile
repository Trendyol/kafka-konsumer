## help: print this help message
help:
	@echo "Usage:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ":" | sed -e 's/^/  /'

## lint: runs golangci lint based on .golangci.yml configuration
.PHONY: lint
lint:
	@if ! test -f `go env GOPATH`/bin/golangci-lint; then go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.0; fi
	golangci-lint run -c .golangci.yml --fix -v

## test: runs tests
.PHONY: test
test:
	go test -v ./... -coverprofile=unit_coverage.out -short

## unit-coverage-html: extract unit tests coverage to html format
.PHONY: unit-coverage-html
unit-coverage-html:
	make test
	go tool cover -html=unit_coverage.out -o unit_coverage.html

## godoc: generate documentation
.PHONY: godoc
godoc:
	@if ! test -f `go env GOPATH`/bin/godoc; then go install golang.org/x/tools/cmd/godoc; fi
	godoc -http=127.0.0.1:6060

## integration-compose: up compose for integration test suite
.PHONY: integration-compose
integration-compose:
	docker compose -f test/integration/docker-compose.yml up --wait --build --force-recreate --remove-orphans

## integration-test: run integration test
.PHONE: integration-test
integration-test:
	go test -v test/integration/integration_test.go

## run-act: act for running github actions on your local machine
run-act:
	act -j test --container-architecture linux/arm64