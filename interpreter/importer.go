package interpreter

import "github.com/gusakk/flux/semantic"

// Importer produces a package given an import path
type Importer interface {
	semantic.Importer
	ImportPackageObject(path string) (*Package, bool)
}
