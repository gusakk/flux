// +build !go1.12

package runtime

import (
	"github.com/gusakk/flux/values"
)

func Version() (values.Value, error) {
	return nil, errBuildInfoNotPresent
}
