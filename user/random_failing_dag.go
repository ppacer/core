package user

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/dskrzypiec/scheduler/dag"
	"github.com/dskrzypiec/scheduler/user/tasks"
)

type RandomFailTask struct {
	Name string
}

func (rft RandomFailTask) Id() string { return rft.Name }

func (rft RandomFailTask) Execute() {
	r := rand.Intn(10)
	if r >= 6 {
		panic("BAD LUCK!")
	}
	fmt.Println("[RandomFailTask] You're lucky!")
}

func createRandomFailDag(n int) dag.Dag {
	root := dag.Node{Task: tasks.PrintTask{Name: "start"}}
	prev := &root
	for i := 0; i < n; i++ {
		node := dag.Node{Task: RandomFailTask{Name: fmt.Sprintf("rft_%d", i)}}
		prev.Next(&node)
		prev = &node
	}

	startTs := time.Date(2023, time.November, 25, 15, 0, 0, 0, time.UTC)
	sched := dag.FixedSchedule{Interval: 60 * time.Second, Start: startTs}
	attr := dag.Attr{Tags: []string{"test"}}
	d := dag.New(dag.Id("random_failing_dag")).
		AddSchedule(&sched).
		AddRoot(&root).
		AddAttributes(attr).
		Done()
	return d
}
