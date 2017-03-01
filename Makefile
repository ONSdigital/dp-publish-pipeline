SERVICES?=publish-receiver publish-scheduler publish-metadata publish-tracker publish-data \
    content-api generator-api publish-search-indexer
UTILS?=decrypt kafka s3 utils
PACKABLE_BIN=scripts/dp
PACKABLE_ETC=doc/init.sql
REMOTE_BIN=bin
REMOTE_ETC=etc

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
DATE:=$(shell date '+%Y%m%d-%H%M%S')
TGZ_FILE=publish-$(GOOS)-$(GOARCH)-$(DATE).tar.gz

build: test
	@mkdir -p $(BUILD_ARCH) || exit 1; \
	for service in $(SERVICES); do \
		echo Building $$service; \
		cd $$service || exit 1; \
		go build -o ../$(BUILD_ARCH)/$(REMOTE_BIN)/$$service || exit 1; \
		cd - > /dev/null || exit 1; \
	done

test:
	@rc=0; for service in $(SERVICES) $(UTILS); do \
		echo Testing $$service ...; \
		cd $$service || exit 1; \
		go test || rc=$?; \
		cd - > /dev/null || exit 2; \
	done; exit $$rc

clean:
	rm -r $(BUILD_ARCH)

producer:
	kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.schedule
$(SERVICES):
	@cd $@ && echo go run $@.go
all: $(SERVICES)

# target AWS:                   make package GOOS=linux GOARCH=amd64
# target AWS, build on Mac:     make package GOOS=linux
package: build
	mkdir -p $(BUILD_ARCH)/$(REMOTE_ETC)
	for i in $(PACKABLE_ETC); do cp -p $$i $(BUILD_ARCH)/$(REMOTE_ETC); done
	for i in $(PACKABLE_BIN); do cp -p $$i $(BUILD_ARCH)/$(REMOTE_BIN); done
	tar -zcf $(TGZ_FILE) -C $(BUILD_ARCH) .

.PHONY: build package producer test all
