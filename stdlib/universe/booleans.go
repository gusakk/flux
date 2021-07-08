package universe

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/values"
)

func init() {
	flux.RegisterPackageValue("universe", "true", values.NewBool(true))
	flux.RegisterPackageValue("universe", "false", values.NewBool(false))
}
