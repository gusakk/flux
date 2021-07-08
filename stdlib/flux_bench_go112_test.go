// +build !go1.13

package stdlib_test

import (
	"testing"

	"github.com/gusakk/flux"
)

func reportStatistics(b *testing.B, stats flux.Statistics) {
	// Not supported in go 1.12.
}
