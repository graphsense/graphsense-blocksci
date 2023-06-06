RELEASE := 'v23.06'
RELEASESEM := 'v1.4.0'

all: build

build:
	docker build -t blocksci .

tag-version:
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all build tag-version