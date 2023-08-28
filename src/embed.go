package src

import "embed"

const ModuleName = "go_sched/src/"

//go:embed dag/*.go meta/*.go user/*.go user/tasks/*.go version/*.go
var GoSourceFiles embed.FS
