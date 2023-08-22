package src

import "embed"

const ModuleName = "go_sched/src/"

//go:embed dag/*.go meta/*.go version/*.go
var GoSourceFiles embed.FS
