package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/ast"
	_ "github.com/gusakk/flux/builtin"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/dependencies/filesystem"
	"github.com/gusakk/flux/internal/errors"
	"github.com/gusakk/flux/lang"
	"github.com/gusakk/flux/memory"
	"github.com/gusakk/flux/parser"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "test_rewriter",
	Short: "A tool for fixing common problems with Flux tests by rewriting them in-place.",
	Long:  "A tool for fixing common problems with Flux tests by rewriting them in-place.",
}

func init() {
	rootCmd.Flags().BoolVar(&flagDryRun, "dry-run", false, "do nothing, but show what would be done")
}

var (
	flagDryRun = false
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func doSubCommand(f func(fileName string) error, args []string) error {
	for _, arg := range args {
		fmt.Printf("%v:\n", arg)
		if err := f(arg); err != nil {
			return errors.Wrap(err, codes.Inherit, arg)
		}
		fmt.Println()
	}
	return nil
}

func getFileAST(fileName string) (*ast.Package, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	script, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	astPkg := parser.ParseSource(string(script))
	return astPkg, nil

}

func runQuery(query string) (flux.ResultIterator, error) {
	c := lang.FluxCompiler{
		Extern: nil,
		Query:  query,
	}
	deps := flux.NewDefaultDependencies()
	deps.Deps.FilesystemService = filesystem.SystemFS

	ctx := deps.Inject(context.Background())
	program, err := c.Compile(ctx)
	if err != nil {
		return nil, err
	}
	ctx = deps.Inject(ctx)
	alloc := &memory.Allocator{}
	qry, err := program.Start(ctx, alloc)
	if err != nil {
		return nil, err
	}
	return flux.NewResultIteratorFromQuery(qry), nil
}

func rewriteFile(fileName string, astPkg *ast.Package) error {
	newScript := ast.Format(astPkg) + "\n"
	if !flagDryRun {
		if err := ioutil.WriteFile(fileName, []byte(newScript), 0644); err != nil {
			return err
		}
	}
	return nil
}
