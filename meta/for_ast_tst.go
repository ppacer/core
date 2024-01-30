// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

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
