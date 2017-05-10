SERVICES?=publish-receiver publish-scheduler publish-metadata publish-tracker publish-data \
     publish-search-indexer publish-deleter
SKIP_SERVICES?=
UTILS?=decrypt kafka s3 utils
PACKABLE_BIN=scripts/dp scripts/ennary
PACKABLE_ETC=scripts/init.sql
REMOTE_BIN=bin
REMOTE_ETC=etc
ANSIBLE_ARGS?=
ARCHIVE?=$(shell make $(MAKEFLAGS) latest-archive)
HASH?=$(shell make hash)
CMD_DIR?=cmd

S3_BUCKET?=dp-publish-content-test
S3_RELEASE_FOLDER?=release
S3_URL?=s3://$(S3_BUCKET)/$(S3_RELEASE_FOLDER)

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
DATE:=$(shell date '+%Y%m%d-%H%M%S')
TGZ_FILE=publish-$(GOOS)-$(GOARCH)-$(DATE)-$(HASH).tar.gz

build:
	@mkdir -p $(BUILD_ARCH) || exit 1; \
	for service in $(SERVICES); do \
		[[ " $(SKIP_SERVICES) " = *" $$service "* ]] && continue; \
		echo Building $$service; \
		main=$(CMD_DIR)/$$service/main.go; \
		[[ -f $$main ]] || exit 1; \
		go build -o $(BUILD_ARCH)/$(REMOTE_BIN)/$$service $$main || exit 1; \
	done

test:
	@rc=0; for service in $(SERVICES) $(UTILS); do \
		[[ " $(SKIP_SERVICES) " = *" $$service "* ]] && continue; \
		main=$(CMD_DIR)/$$service/main.go; \
		[[ -f $$main ]] || exit 1; \
		echo Testing $$service ...; \
		go test $$main || rc=$?; \
	done; exit $$rc

clean:
	[[ -n "$(BUILD)" && -d "$(BUILD)" ]] && rm -r $(BUILD)/*

producer:
	kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.schedule
$(SERVICES):
	@main=$(CMD_DIR)/$@/main.go; if [[ ! -f $$main ]]; then echo Cannot see $$main; exit 1; fi; go run -race $$main
all: $(SERVICES)

hash:
	@git rev-parse --short HEAD

# target AWS:                   make package GOOS=linux GOARCH=amd64
# target AWS, build on Mac:     make package GOOS=linux
package: build
	mkdir -p $(BUILD_ARCH)/$(REMOTE_ETC)
	for i in $(PACKABLE_ETC); do cp -p $$i $(BUILD_ARCH)/$(REMOTE_ETC); done
	for i in $(PACKABLE_BIN); do cp -p $$i $(BUILD_ARCH)/$(REMOTE_BIN); done
	tar -zcf $(TGZ_FILE) -C $(BUILD_ARCH) .

latest-archive:
	@ls publish-$(GOOS)-$(GOARCH)-*.tar.gz | tail -1

# deploy AWS, package on Mac:   make deploy GOOS=linux
deploy: upload-build deploy-archive
upload-build:
	@test -n "$(ARCHIVE)" && test -f "$(ARCHIVE)" && aws s3 cp $(ARCHIVE) $(S3_URL)/
deploy-archive:
	archive=$(ARCHIVE); test -n "$$archive" && cd ../dp-setup/ansible && \
	ansible-playbook $(ANSIBLE_ARGS) -i prototype_hosts prototype.yml -e "s3_bucket=$(S3_BUCKET) s3_archive_file=$(S3_RELEASE_FOLDER)/$$archive archive_file=$$archive"

nomad:
		@for t in nomad/*-template.nomad; do			\
			plan=$${t%-template.nomad}.nomad;	\
			test -f $$plan && rm $$plan;		\
			sed	-e 's,NOMAD_DATA_CENTER,$(DATA_CENTER),g'			\
				-e 's,S3_TAR_FILE_LOCATION,$(S3_TAR_FILE),g'			\
				-e 's,KAFKA_ADDRESS,$(KAFKA_ADDR),g'		\
				-e 's,VAULT_ADDRESS,$(VAULT_ADDR),g'		\
				-e 's,S3_CONTENT_URL,$(S3_URL),g'		\
				-e 's,S3_CONTENT_BUCKET,$(S3_BUCKET),g'	\
				-e 's,UPSTREAM_S3_CONTENT_BUCKET,$(UPSTREAM_S3_BUCKET),g'	\
				-e 's,UPSTREAM_S3_CONTENT_URL,$(UPSTREAM_S3_URL),g'	\
				-e 's,PUBLISH_DB_ACCESS,$(PUBLISH_DB_ACCESS),g'		\
				-e 's,WEB_DB_ACCESS,$(WEB_DB_ACCESS),g'		\
				-e 's,SCHEDULER_VAULT_TOKEN,$(SCHEDULER_VAULT_TOKEN),g'		\
				-e 's,ELASTIC_SEARCH_URL,$(ELASTIC_SEARCH_URL),g'		\
				< $$t > $$plan || exit 2;			\
		done

.PHONY: build package producer test all latest-archive deploy deploy-archive upload-build nomad $(SERVICES)
