package execute

import (
	"io"

	"github.com/gusakk/flux"
	"github.com/gusakk/flux/values"
)

type RowReader interface {
	Next() bool
	GetNextRow() ([]values.Value, error)
	ColumnNames() []string
	ColumnTypes() []flux.ColType
	SetColumns([]interface{})
	io.Closer
}
