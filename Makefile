fmt:
	go fmt

test:
	go test -coverprofile=testresults/testcoverage.txt -tags "icu json1 fts5" ./...
	go tool cover -html=testresults/testcoverage.txt -o testresults/testcoverage.html

benchmark:
	go test -bench=. -count=5 -benchmem -tags "icu json1 fts5" -run=^$  ./... | tee testresults/testperf.txt

buid: test
	go build -tags "icu json1 fts5"

install: test
	go install -tags "icu json1 fts5"