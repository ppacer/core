echo 'go build...'
go build ./...

echo 'unit tests...'
go list ./... | grep -v e2etests | xargs go test -count=1 -cover

echo 'end-to-end tests...'
go test -count=1 -cover ./e2etests

echo 'benchmarks...'
go test -bench=. -benchmem ./ds ./dag/schedule
