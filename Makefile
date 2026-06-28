run:
	go run main.go

tidy:
	go mod tidy

build:
	go build -o bootstrap .

vet:
	go vet ./...

test:
	go test -v -race -timeout 30s ./...

bench:
	go test -bench=. -benchmem -count=3 ./...

bench-cpu:
	go test -bench=. -cpuprofile=cpu.prof ./...
	go tool pprof cpu.prof

bench-mem:
	go test -bench=. -memprofile=mem.prof ./...
	go tool pprof mem.prof

.PHONY: run tidy build vet test bench bench-cpu bench-mem
