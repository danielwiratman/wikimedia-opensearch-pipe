build:
	go build -o bin/extractor cmd/extractor/main.go
	go build -o bin/loader cmd/loader/main.go

extractor: build
	bin/extractor

loader: build
	bin/loader

clean:
	rm -rf bin
