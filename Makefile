fmt:
	go fmt

test:
	go test -tags "icu json1 fts5" ./...

buid:
	go build -tags "icu json1 fts5"

install:
	go install -tags "icu json1 fts5"