package slack

import (
	"context"
	"strconv"
	"strings"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

var defaultColors = map[string]struct{}{
	"good":    {},
	"warning": {},
	"danger":  {},
}

var errColorParse = errors.New(codes.Invalid, "could not parse color string")

func validateColorString(color string) error {
	if _, ok := defaultColors[color]; ok {
		return nil
	}

	if strings.HasPrefix(color, "#") {
		hex, err := strconv.ParseInt(color[1:], 16, 64)
		if err != nil {
			return err
		}
		if hex < 0 || hex > 0xffffff {
			return errColorParse
		}
		return nil
	}
	return errColorParse
}

var validateColorStringFluxFn = values.NewFunction(
	"validateColorString",
	semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{"color": semantic.String},
		Required:   semantic.LabelSet{"color"},
		Return:     semantic.String,
	}),
	func(ctx context.Context, args values.Object) (values.Value, error) {
		v, ok := args.Get("color")

		if !ok {
			return nil, errors.New(codes.Invalid, "missing argument: color")
		}

		if v.Type().Nature() == semantic.String {
			if err := validateColorString(v.Str()); err != nil {
				return nil, err
			}
			return v, nil
		}

		return nil, errColorParse
	},
	false,
)

func init() {
	flux.RegisterPackageValue("slack", "validateColorString", validateColorStringFluxFn)
}
