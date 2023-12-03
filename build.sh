rm test.db
sqlite3 test.db < schema.sql
go generate
go test -count=1 -cover ./...
go test -bench=. -benchmem ./ds
go build -o ./bin/scheduler ./cmd/scheduler
go build -o ./bin/executor ./cmd/executor
