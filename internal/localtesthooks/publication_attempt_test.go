package localtesthooks

import (
	"context"
	"testing"
)

func TestPublicationAttemptOptInIsIsolated(t *testing.T) {
	for _, ctx := range []context.Context{context.Background(), context.WithValue(context.Background(), "publicationAttemptKey", "unrelated")} {
		if got := PublicationAttempt(ctx); got.Started != nil || got.Before != nil || got.After != nil {
			t.Fatal("ordinary or unrelated context installed an observer")
		}
	}
	called := false
	with := WithPublicationAttempt(context.Background(), PublicationAttemptObserver{Started: func() { called = true }})
	got := PublicationAttempt(with)
	if got.Started == nil {
		t.Fatal("explicit runtime observer was not installed")
	}
	got.Started()
	if !called {
		t.Fatal("installed observer did not run")
	}
	if other := PublicationAttempt(context.Background()); other.Started != nil {
		t.Fatal("observer leaked into another context")
	}
}
