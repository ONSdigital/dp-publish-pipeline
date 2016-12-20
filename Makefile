SVCS=receiver scheduler sender tracker migrator

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
DATE:=$(shell date '+%Y%m%d-%H%M%S')

build:
	@mkdir -p $(BUILD_ARCH) || exit 1; \
	for i in $(SVCS); do \
		echo Building $$i; \
		src=publish-$$i; dir=$$src; if [ $$i = migrator ]; then src=content-$$i; dir=static-$$src; fi; \
		cd $$dir || exit 1; \
		go build -o ../$(BUILD_ARCH)/$$src $$src.go || exit 1; \
		cd - > /dev/null || exit 1; \
	done

producer:
	kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.schedule
$(SVCS):
	src=publish-$@; dir=$$src; if [ $@ = migrator ]; then src=content-$@; dir=static-$$src; fi; \
	cd $$dir && go run $$src.go

# target AWS:                   make package GOOS=linux GOARCH=amd64
# target AWS, build on Mac:     make package GOOS=linux
package: build
	tar -zcf publish-$(GOOS)-$(GOARCH)-$(DATE).tar.gz -C $(BUILD_ARCH) .

.PHONY: build package producer
