package meta

import "fmt"

type PointTest struct {
	X int
	Y int
}

func (pt PointTest) String() string {
	return fmt.Sprintf("Point(%d, %d)", pt.X, pt.Y)
}

func (pt *PointTest) EmptyMethod() {
}

func (pt PointTest) privateMethod() int {
	return 42
}
