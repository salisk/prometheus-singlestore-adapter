VERSION=$(shell git describe --always | sed 's|v\(.*\)|\1|')
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
OS:=$(shell uname -s | awk '{ print tolower($$1) }')
ORGANIZATION=timescale

ARCH=$(shell go env GOARCH)
ifeq ($(ARCH),)
  ARCH=amd64
endif
ifeq ($(shell uname -m), i386)
	ARCH=386
endif

# Packages for running tests
PKGS:= $(shell go list ./... | grep -v /vendor)

SOURCES:=$(shell find . -name '*.go'  | grep -v './vendor')

TARGET:=prometheus-postgresql-adapter

.PHONY: all clean build docker-image docker-push test

all: $(TARGET) version.properties

version.properties:
	@echo "version=${VERSION}" > version.properties

.target_os:
	@echo $(OS) > .target_os

build: $(TARGET)

$(TARGET): .target_os $(SOURCES)
	GOOS=$(OS) GOARCH=${ARCH} CGO_ENABLED=0 go build -a --ldflags '-w' -o bin/$@ ./cmd/$@

docker-image: version.properties
	docker build -t $(ORGANIZATION)/$(TARGET):latest .
	docker tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):${VERSION}
	docker tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):${BRANCH}

docker-push: docker-image
	docker push $(ORGANIZATION)/$(TARGET):latest
	docker push $(ORGANIZATION)/$(TARGET):${VERSION}
	docker push $(ORGANIZATION)/$(TARGET):${BRANCH}

test:
	go clean -testcache $(PKGS)
	go test -v -race $(PKGS)

clean:
	go clean $(PKGS)
	rm -f *~ $(TARGET) version.properties .target_os
