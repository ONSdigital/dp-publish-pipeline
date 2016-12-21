SERVICES?=receiver scheduler sender tracker migrator
UTILS?=decrypt kafka s3 utils

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
DATE:=$(shell date '+%Y%m%d-%H%M%S')

# common function to translate SERVICES/UTILS arguments into $$dir and $$src:
#   - $$dir/$$src.go exists if arg is in $(SERVICES) (generally add prefix 'publish-' to $$i)
#   - $$dir = arg, if arg is in $(UTILS)
SET_VARS=set_vars(){ local repo=$1; shift; if [[ " $(UTILS) " = *" $$i "* ]];then dir=$$i; src=na; return; fi; src=publish-$$i; dir=$$src; if [ $$i = migrator ]; then src=content-$$i; dir=static-$$src; fi; }

build: test
	@mkdir -p $(BUILD_ARCH) || exit 1; \
	$(SET_VARS); for i in $(SERVICES); do set_vars $$i; \
		echo Building $$i; \
		cd $$dir || exit 1; \
		go build -o ../$(BUILD_ARCH)/$$src $$src.go || exit 1; \
		cd - > /dev/null || exit 1; \
	done

test:
	@$(SET_VARS); rc=0; for i in $(SERVICES) $(UTILS); do set_vars $$i; \
		echo Testing $$i ...; \
		cd $$dir || exit 1; \
		go test || rc=$?; \
		cd - > /dev/null || exit 2; \
	done; exit $$rc

producer:
	kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.schedule
$(SERVICES):
	src=publish-$@; dir=$$src; if [ $@ = migrator ]; then src=content-$@; dir=static-$$src; fi; \
	cd $$dir && go run $$src.go

# target AWS:                   make package GOOS=linux GOARCH=amd64
# target AWS, build on Mac:     make package GOOS=linux
package: build
	tar -zcf publish-$(GOOS)-$(GOARCH)-$(DATE).tar.gz -C $(BUILD_ARCH) .

.PHONY: build package producer test
