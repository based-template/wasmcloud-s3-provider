# blobstore-s3 Makefile

CAPABILITY_ID = "auxiliary::interfaces::blobstore"
NAME = "blobstore-s3"
VENDOR = "OMT"
PROJECT = blobstore_s_3
VERSION = $(shell cargo metadata --no-deps --format-version 1 | jq -r '.packages[] .version' | head -1)
REVISION = 0

include ./provider.mk

