package secrets

import (
	"context"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/interpreter"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

const GetKind = "get"

func init() {
	flux.RegisterPackageValue("influxdata/influxdb/secrets", GetKind, GetFunc)
}

// GetFunc is a function that calls Get.
var GetFunc = makeGetFunc()

func makeGetFunc() values.Function {
	sig := semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{
			"key": semantic.String,
		},
		Required: semantic.LabelSet{"key"},
		Return:   semantic.String,
	})
	return values.NewFunction("get", sig, Get, false)
}

// Get retrieves the secret key identifier for a given secret.
func Get(ctx context.Context, args values.Object) (values.Value, error) {
	fargs := interpreter.NewArguments(args)
	key, err := fargs.GetRequiredString("key")
	if err != nil {
		return nil, err
	}

	ss, err := flux.GetDependencies(ctx).SecretService()
	if err != nil {
		return nil, errors.Wrapf(err, codes.Inherit, "cannot retrieve secret %q", key)
	}

	value, err := ss.LoadSecret(ctx, key)
	if err != nil {
		return nil, err
	}
	return values.NewString(value), nil
}
