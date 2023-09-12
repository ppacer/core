rm test.db
sqlite3 test.db < schema.sql
go generate
go test -count=1 ./...
go build -o ./bin/scheduler ./cmd/scheduler
go build -o ./bin/executor ./cmd/executor
