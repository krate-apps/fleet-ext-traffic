.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/traffic-worker ./cmd/traffic-worker

pack:
	bash scripts/pack.sh dist
