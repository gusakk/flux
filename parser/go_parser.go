// +build !libflux

package parser

import (
	"fmt"

	"github.com/gusakk/flux/ast"
	"github.com/gusakk/flux/internal/parser"
	"github.com/gusakk/flux/internal/token"
)

func parseFile(f *token.File, src []byte) (*ast.File, error) {
	if useRustParser() {
		panic(fmt.Sprintf(`%v set to %q but this is not a libflux build`, fluxParserTypeEnvVar, parserTypeRust))
	}
	return parser.ParseFile(f, src), nil
}

func isLibfluxBuild() bool {
	return false
}
