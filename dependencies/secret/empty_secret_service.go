package secret

import (
	"context"
	"github.com/gusakk/flux/codes"
	"github.com/gusakk/flux/internal/errors"
)

func (ess EmptySecretService) LoadSecret(ctx context.Context, k string) (string, error) {
	return "", errors.Newf(codes.NotFound, "secret key %q not found", k)
}

// Secret service that always reports no secrets exist
type EmptySecretService struct {
}
