package executetest

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/dependencies/dependenciestest"
)

func NewTestExecuteDependencies() flux.Dependencies {
	return dependenciestest.Default()
}
