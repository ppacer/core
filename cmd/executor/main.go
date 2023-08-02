package main

import (
	"fmt"
	"go_shed/dag"
)

func main() {
	attr := dag.Attr{Id: "crap", Schedule: ""}
	fmt.Println("Executor")
	fmt.Printf("%v\n", attr)
}
