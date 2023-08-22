package src

import "embed"

//go:embed dag/*.go meta/*.go version/*.go
var goSourceFiles embed.FS
