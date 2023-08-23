go generate
go test ./...
go build -o ./bin/scheduler ./cmd/scheduler
go build -o ./bin/executor ./cmd/executor
