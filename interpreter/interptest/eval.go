package interptest

import (
	"context"

	"github.com/gusakk/flux/ast"
	"github.com/gusakk/flux/interpreter"
	"github.com/gusakk/flux/parser"
	"github.com/gusakk/flux/semantic"
	"github.com/gusakk/flux/values"
)

func Eval(ctx context.Context, itrp *interpreter.Interpreter, scope values.Scope, importer interpreter.Importer, src string) ([]interpreter.SideEffect, error) {
	pkg := parser.ParseSource(src)
	if ast.Check(pkg) > 0 {
		return nil, ast.GetError(pkg)
	}
	node, err := semantic.New(pkg)
	if err != nil {
		return nil, err
	}
	return itrp.Eval(ctx, node, scope, importer)
}
