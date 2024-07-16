// Copyright 2023 The ppacer Authors.
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

package pace

import "time"

// Fixed implements Strategy for fixed-duration intervals. Reset methods does
// nothing. All intervals are the same duration.
type Fixed struct {
	interval time.Duration
}

// NewFixed initialize new Fixed strategy for given interval duration.
func NewFixed(interval time.Duration) *Fixed {
	return &Fixed{interval: interval}
}

// NextInterval returns fixed interval duration every time.
func (f *Fixed) NextInterval() time.Duration { return f.interval }

// Reset does nothing for Fixed strategy.
func (f *Fixed) Reset() {}
