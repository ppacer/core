go generate
go build ./...
go test -count=1 -parallel 4 -cover ./...
go test -bench=. -benchmem ./ds
