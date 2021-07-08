package experimental_test

import (
	"context"
	"testing"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/dependencies/dependenciestest"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

func addFail(scope values.Scope) {
	scope.Set("fail", values.NewFunction(
		"fail",
		semantic.NewFunctionPolyType(semantic.FunctionPolySignature{
			Return: semantic.Bool,
		}),
		func(ctx context.Context, args values.Object) (values.Value, error) {
			return nil, errors.New(codes.Aborted, "fail")
		},
		false,
	))
}

func TestObjectKeys(t *testing.T) {
	script := `
import "experimental"

o = {a: 1, b: 2, c: 3}
experimental.objectKeys(o: o) == ["a", "b", "c"] or fail()
`
	ctx := dependenciestest.Default().Inject(context.Background())
	if _, _, err := flux.Eval(ctx, script, addFail); err != nil {
		t.Fatal("evaluation of objectKeys failed: ", err)
	}
}
