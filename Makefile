fmt:
	go fmt

test:
	go test -coverprofile=testresults/testcoverage.txt -tags "json1 vtable" -race ./...
	go tool cover -html=testresults/testcoverage.txt -o testresults/testcoverage.html

benchmark:
	go test -bench=. -count=5 -benchmem -tags "json1 vtable" -run=^$  ./... | tee testresults/testperf.txt

build: test
	go vet -tags "json1 vtable"
	go build -tags "json1 vtable"

install: test
	go vet -tags "json1 vtable"
	go install -tags "json1 vtable"
