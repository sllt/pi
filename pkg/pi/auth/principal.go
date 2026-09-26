// Package auth carries identities established by trusted authentication code.
package auth

import (
	"context"
	"github.com/sllt/pi/pkg/pi/apperror"
)

type Principal struct {
	Subject     string
	Permissions []string
}
type principalKey struct{}

// WithPrincipal is for verified transport adapters and trusted task hosts. Never
// build a Principal directly from request parameters. Both boundaries copy slices.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	p.Permissions = append([]string(nil), p.Permissions...)
	return context.WithValue(ctx, principalKey{}, p)
}
func FromContext(ctx context.Context) (Principal, bool) {
	if ctx == nil {
		return Principal{}, false
	}
	p, ok := ctx.Value(principalKey{}).(Principal)
	p.Permissions = append([]string(nil), p.Permissions...)
	return p, ok && p.Subject != ""
}
func Require(ctx context.Context) (Principal, error) {
	p, ok := FromContext(ctx)
	if !ok {
		return Principal{}, apperror.New(apperror.Unauthenticated, 401, "Unauthorized")
	}
	return p, nil
}
func (p Principal) Can(permission string) bool {
	for _, v := range p.Permissions {
		if v == permission {
			return true
		}
	}
	return false
}

// AuthorizeSubject defaults an empty target to the caller. Cross-subject access
// needs an explicit use-case permission; an empty permission never grants it.
func AuthorizeSubject(ctx context.Context, target, permission string) (string, error) {
	p, err := Require(ctx)
	if err != nil {
		return "", err
	}
	if target == "" {
		target = p.Subject
	}
	if target != p.Subject && (permission == "" || !p.Can(permission)) {
		return "", apperror.New(apperror.Forbidden, 403, "Forbidden")
	}
	return target, nil
}
