rm test.db
sqlite3 test.db < schema.sql
go generate
go test -count=1 ./...
go test -bench=. -benchmem ./src/meta
go test -bench=. -benchmem ./src/ds
go build -o ./bin/scheduler ./cmd/scheduler
go build -o ./bin/executor ./cmd/executor
