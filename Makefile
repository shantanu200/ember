run:
	go run main.go

tidy:
	go mod tidy

build:
	go build -o bootstrap .

vet:
	go vet ./...

test:
	go test ./... -v -timeout 30s
