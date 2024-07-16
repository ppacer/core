// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package pace

import (
	"testing"
	"time"
)

func TestFixed(t *testing.T) {
	data := []time.Duration{
		1 * time.Millisecond,
		15 * time.Nanosecond,
		15 * 60 * time.Second,
	}

	for _, input := range data {
		f := NewFixed(input)
		if next := f.NextInterval(); next != input {
			t.Errorf("Expected NextInterval %v, got: %v",
				input, next)
		}
		f.Reset()
		if next := f.NextInterval(); next != input {
			t.Errorf("Expected NextInterval %v, got: %v",
				input, next)
		}
	}
}
