fmt:
	go fmt

test:
	go test -coverprofile=testresults/testcoverage.txt -tags "icu json1 fts5 vtable" -race ./...
	go tool cover -html=testresults/testcoverage.txt -o testresults/testcoverage.html

benchmark:
	go test -bench=. -count=5 -benchmem -tags "icu json1 fts5 vtable" -run=^$  ./... | tee testresults/testperf.txt

build: test
	go build -tags "icu json1 fts5 vtable"

install: test
	go install -tags "icu json1 fts5 vtable"