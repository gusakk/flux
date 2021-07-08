package universe

import (
	"context"
	"time"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/interpreter"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

func init() {
	flux.RegisterPackageValue("universe", "sleep", sleepFunc)
}

const (
	vArg        = "v"
	durationArg = "duration"
)

var sleepFunc = values.NewFunction(
	"sleep",
	semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{
			vArg:        semantic.Tvar(1),
			durationArg: semantic.Duration,
		},
		PipeArgument: vArg,
		Required:     semantic.LabelSet{vArg, durationArg},
		Return:       semantic.Tvar(1),
	}),
	func(ctx context.Context, args values.Object) (values.Value, error) {
		return interpreter.DoFunctionCallContext(sleep, ctx, args)
	},
	// sleeping is a side effect
	true,
)

func sleep(ctx context.Context, args interpreter.Arguments) (values.Value, error) {
	v, err := args.GetRequired(vArg)
	if err != nil {
		return nil, err
	}

	// TODO(jsternberg): There should be a GetRequiredDuration, but
	// that would cause a breaking change and the commit this is getting
	// added to is meant to be a patch fix. Come back here later when
	// Arguments can be refactored in a breaking way to make it not an
	// interface.
	d, err := args.GetRequired(durationArg)
	if err != nil {
		return nil, err
	} else if d.Type().Nature() != semantic.Duration {
		return nil, errors.Newf(codes.Invalid, "keyword argument %q should be of kind %v, but got %v", durationArg, semantic.Duration, v.PolyType().Nature())
	}

	timer := time.NewTimer(d.Duration().Duration())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return v, nil
	}
}
