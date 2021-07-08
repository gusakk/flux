package langtest

import (
	"github.com/gusakk/flux/lang"
	"github.com/gusakk/flux/memory"
)

func DefaultExecutionDependencies() lang.ExecutionDependencies {
	return lang.ExecutionDependencies{
		Allocator: new(memory.Allocator),
	}
}
