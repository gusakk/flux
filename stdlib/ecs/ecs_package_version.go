package ecs

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/values"
)

// required only to get package registered
func init() {
	flux.RegisterPackageValue("ecs", "ecs_package_version", values.NewUInt(1))
}
