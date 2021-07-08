package arrow

import (
	arrowmemory "github.com/apache/arrow/go/arrow/memory"
	"github.com/gusakk/flux/memory"
)

func NewAllocator(a *memory.Allocator) arrowmemory.Allocator {
	return a
}
