vendor: go.mod
	@echo "==> Building required libraries in vendor ..."
	@go mod vendor

fmt:
	@echo "==> Formatting go files ..."
	@go fmt ./...

clean:
	@echo "==> Cleaning all header and so files ..."
	@rm -rf **/*.h
	@rm -rf **/*.so

shared:
	@echo "==> Building shared libraries for mac ..."
	@CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o ./sqle/ddl2json_darwin.so -buildmode=c-shared main.go

current-dir:
	$(eval DIR := $(shell pwd))

docker-shared-linux: current-dir
	@echo "==> Building shared libraries for linux ..."
	@docker run -it --rm --name docker-build-linux-dll -v "$(DIR)":/usr/src/go -w /usr/src/go  golang:1.16-stretch make shared-linux

shared-linux:
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -mod vendor -o ./sqle/ddl2json_linux.so -buildmode=c-shared main.go

build: fmt clean shared docker-shared-linux

.PHONY: shared-linux shared
