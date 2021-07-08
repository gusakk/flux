package system

import (
	"context"
	"time"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

var systemTimeFuncName = "time"

func init() {
	flux.RegisterPackageValue("system", systemTimeFuncName, values.NewFunction(
		systemTimeFuncName,
		semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
			Return: semantic.Time,
		}),
		func(ctx context.Context, args values.Object) (values.Value, error) {
			return values.NewTime(values.ConvertTime(time.Now().UTC())), nil
		},
		false,
	))
}
