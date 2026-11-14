package localtesthooks

import "context"

type publicationAttemptKey struct{}

type PublicationAttemptObserver struct {
	Started func()
	Before  func(context.Context)
	After   func(context.Context, error)
}

// WithPublicationAttempt observes one runtime's completed checkpoint attempt.
// A test callback may park the worker, but must return when its lifetime ends.
func WithPublicationAttempt(ctx context.Context, observe PublicationAttemptObserver) context.Context {
	return context.WithValue(ctx, publicationAttemptKey{}, observe)
}

func PublicationAttempt(ctx context.Context) PublicationAttemptObserver {
	observe, _ := ctx.Value(publicationAttemptKey{}).(PublicationAttemptObserver)
	return observe
}
