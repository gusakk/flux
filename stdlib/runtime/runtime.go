package runtime

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

const versionFuncName = "version"

var errBuildInfoNotPresent = errors.New(codes.NotFound, "build info is not present")

func init() {
	flux.RegisterPackageValue("runtime", versionFuncName, values.NewFunction(
		versionFuncName,
		semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
			Return: semantic.String,
		}),
		Version,
		false,
	))
}
