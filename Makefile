SHELL=bash

SERVICES?=publish-receiver publish-scheduler publish-metadata publish-tracker publish-data \
     publish-search-indexer publish-deleter
SKIP_SERVICES?=
UTILS?=decrypt kafka s3 utils

REMOTE_BIN=bin
REMOTE_ETC=etc
ANSIBLE_ARGS?=
ARCHIVE?=$(shell make $(MAKEFLAGS) latest-archive)
HASH?=$(shell make hash)
CMD_DIR?=cmd
HEALTHCHECK_ENDPOINT?=/healthcheck
DEV?=

S3_BUCKET?=dp-publish-content-test
S3_RELEASE_FOLDER?=release
S3_URL?=s3://$(S3_BUCKET)/$(S3_RELEASE_FOLDER)
S3_REGION?=eu-west-1

ifdef DEV
S3_SECURE?=0
UPSTREAM_S3_SECURE?=0
HUMAN_LOG?=1
DATA_CENTER?=dc1
PACKABLE_BIN?=scripts/dp scripts/ennary
PACKABLE_ETC?=scripts/init.sql
NOMAD?=
else
S3_SECURE?=1
UPSTREAM_S3_SECURE?=1
HUMAN_LOG?=
DATA_CENTER?=$(S3_REGION)
PACKABLE_BIN?=
PACKABLE_ETC?=
NOMAD?=1
endif

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)
thisOS:=$(shell uname -s)

ifeq ($(thisOS),Darwin)
SED?=gsed
else
SED?=sed
endif

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
DATE:=$(shell date '+%Y%m%d-%H%M%S')
TGZ_FILE=publish-$(GOOS)-$(GOARCH)-$(DATE)-$(HASH).tar.gz

NOMAD_SRC_DIR?=nomad
NOMAD_PLAN_TARGET?=$(BUILD)

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
		[[ -f $$main ]] || continue; \
		echo Testing $$service ...; \
		go test $$main || rc=$$?; \
	done; exit $$rc

clean:
	[[ -n "$(BUILD)" && -d "$(BUILD)" ]] && rm -r $(BUILD)/*

producer:
	kafka-console-producer --broker-list localhost:9092 --topic uk.gov.ons.dp.web.schedule
$(SERVICES):
ifdef NOMAD
	@nomad_plan=$(NOMAD_PLAN_TARGET)/$@.nomad; if [[ ! -f $$nomad_plan ]]; then echo Cannot see $$nomad_plan; exit 1; fi; echo nomad run $$nomad_plan; nomad run $$nomad_plan
else
	@main=$(CMD_DIR)/$@/main.go; if [[ ! -f $$main ]]; then echo Cannot see $$main; exit 1; fi; go run -race $$main
endif
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
	@test -d $(NOMAD_PLAN_TARGET) || mkdir -p $(NOMAD_PLAN_TARGET)
	@driver=exec; [[ -n "$(DEV)" ]] && driver=raw_exec;	\
	for nomad_template in $(NOMAD_SRC_DIR)/*-template.nomad; do		\
		nomad_target=$(NOMAD_PLAN_TARGET)/$${nomad_template##*/};	\
		nomad_target=$${nomad_target%-*}.nomad;		\
		test -f $$nomad_target && rm $$nomad_target;			\
		$(SED) -r	\
			-e 's,\bNOMAD_DATA_CENTER\b,$(DATA_CENTER),g'			\
			-e 's,\bS3_TAR_FILE_LOCATION\b,$(S3_TAR_FILE),g'		\
			-e 's,\bKAFKA_ADDRESS\b,$(KAFKA_ADDR),g'			\
			-e 's,\bVAULT_ADDRESS\b,$(VAULT_ADDR),g'			\
			-e 's,\bS3_CONTENT_URL\b,$(S3_URL),g'				\
			-e 's,\bS3_SECURE_FLAG\b,$(S3_SECURE),g'			\
			-e 's,\bS3_CONTENT_BUCKET\b,$(S3_BUCKET),g'			\
			-e 's,\bCOLLECTION_S3_BUCKET\b,$(UPSTREAM_S3_BUCKET),g'		\
			-e 's,\bCOLLECTION_S3_URL\b,$(UPSTREAM_S3_URL),g'		\
			-e 's,\bPUBLISH_DB_ACCESS\b,$(PUBLISH_DB_ACCESS),g'		\
			-e 's,\bWEB_DB_ACCESS\b,$(WEB_DB_ACCESS),g'			\
			-e 's,\bSCHEDULER_VAULT_TOKEN\b,$(SCHEDULER_VAULT_TOKEN),g'	\
			-e 's,\bELASTIC_SEARCH_URL\b,$(ELASTIC_SEARCH_URL),g'		\
			-e 's,\bCOLLECTION_S3_SECURE\b,$(UPSTREAM_S3_SECURE),g'		\
			-e 's,\bHEALTHCHECK_ENDPOINT\b,$(HEALTHCHECK_ENDPOINT),g'	\
			-e 's,\bHUMAN_LOG_FLAG\b,$(HUMAN_LOG),g'			\
			-e 's,^(  *driver  *=  *)"exec",\1"'$$driver'",'		\
			< $$nomad_template > $$nomad_target || exit 2;			\
	done

.PHONY: build package producer test all latest-archive deploy deploy-archive upload-build nomad $(SERVICES)
