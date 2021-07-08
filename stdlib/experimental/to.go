package experimental

import (
	"github.com/gusakk/flux"
	"github.com/gusakk/flux/semantic"
)

// ToKind is the kind for the experimental `to` flux function
const ExperimentalToKind = "experimental-to"

var ToSignature = flux.FunctionSignature(
	map[string]semantic.PolyType{
		"bucket":   semantic.String,
		"bucketID": semantic.String,
		"org":      semantic.String,
		"orgID":    semantic.String,
		"host":     semantic.String,
		"token":    semantic.String,
	},
	[]string{},
)

func init() {
	flux.RegisterPackageValue("experimental", "to", flux.FunctionValueWithSideEffect("to", nil, ToSignature))
}
