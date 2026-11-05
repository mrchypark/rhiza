package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/thanos-io/objstore"
)

type managedCurrentReadBucket struct {
	objstore.Bucket
	attrs                                 int
	beforeGet                             func() error
	attrFailure                           int
	getFailure, bodyFailure, closeFailure error
}

func (b *managedCurrentReadBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.attrs++
	if b.attrs == b.attrFailure {
		return objstore.ObjectAttributes{}, errManagedCurrentRead
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *managedCurrentReadBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.beforeGet != nil {
		if err := b.beforeGet(); err != nil {
			return nil, err
		}
	}
	if b.getFailure != nil {
		return nil, b.getFailure
	}
	r, err := b.Bucket.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return &managedCurrentReadCloser{ReadCloser: r, readErr: b.bodyFailure, closeErr: b.closeFailure}, nil
}

type managedCurrentReadCloser struct {
	io.ReadCloser
	readErr, closeErr error
}

func (r *managedCurrentReadCloser) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return r.ReadCloser.Read(p)
}
func (r *managedCurrentReadCloser) Close() error {
	return errors.Join(r.ReadCloser.Close(), r.closeErr)
}

var errManagedCurrentRead = errors.New("test CURRENT read failure")

func TestManagedCurrentReadFailureDiagnosticScopeAndIdentity(t *testing.T) {
	private := errors.Join(context.Canceled, errors.New("private payload and path"))
	for _, stage := range []string{"attributes_before", "get", "body", "close", "attributes_after", "version_changed", "decode", "policy"} {
		if got := managedCurrentReadFailureDiagnostic(stage, private); got != "MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage="+stage {
			t.Fatal("fixed diagnostic changed or included private error", got)
		}
		if logManagedCurrentReadFailure(stage, private) != private || managedCurrentReadFailureDiagnostic(stage, nil) != "" {
			t.Fatal("diagnostic changed error identity or emitted success")
		}
	}
	for _, stage := range []string{"", "private", "get payload=private", "get\nprivate", "https://private.invalid"} {
		if managedCurrentReadFailureDiagnostic(stage, private) != "" {
			t.Fatal("unsafe stage accepted")
		}
	}
}

func managedCurrentReadFixture(t *testing.T) (*Manager, *managedCurrentReadBucket, ManagedAppendOnlyLocator) {
	t.Helper()
	b := &managedCurrentReadBucket{Bucket: objstore.NewInMemBucket()}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	m, err := NewAppendOnlyManagerForPolicy(b, "current-read", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Pointer-reader unit data is not certificate evidence or activation authority.
	l := ManagedAppendOnlyLocator{Policy: 10, Index: 1, RootHash: sha256.Sum256([]byte("root")), SealDecisionSlot: 2, SealDecisionHash: sha256.Sum256([]byte("seal"))}
	data, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Bucket.Upload(context.Background(), m.key("checkpoint/CURRENT"), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	return m, b, l
}

func TestManagedCurrentReadConcurrentConditionalPublisher(t *testing.T) {
	ctx := context.Background()
	m, b, prior := managedCurrentReadFixture(t)
	next := prior
	next.SealDecisionSlot++
	next.SealDecisionHash = sha256.Sum256([]byte("next-seal"))
	published := false
	b.beforeGet = func() error {
		attr, err := b.Bucket.Attributes(ctx, m.key("checkpoint/CURRENT"))
		if err != nil {
			return err
		}
		data, err := json.Marshal(next)
		if err != nil {
			return err
		}
		err = b.Bucket.Upload(ctx, m.key("checkpoint/CURRENT"), bytes.NewReader(data), objstore.WithIfMatch(attr.Version))
		published = err == nil
		return err
	}
	got, exists, err := m.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if !published || err == nil || !strings.Contains(err.Error(), "CURRENT changed during read") || exists || got != (ManagedAppendOnlyLocator{}) {
		t.Fatalf("wrong concurrent-publisher rejection boundary: published=%t exists=%t err=%v", published, exists, err)
	}
	b.beforeGet = nil
	got, exists, err = m.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || got != next {
		t.Fatal("complete newly published locator was not retained", err)
	}
}

func TestManagedCurrentReadPreservesIOAndValidationRefusals(t *testing.T) {
	for _, mode := range []string{"attributes-before", "get", "body", "close", "attributes-after", "decode", "policy"} {
		t.Run(mode, func(t *testing.T) {
			m, b, _ := managedCurrentReadFixture(t)
			wantIO := true
			switch mode {
			case "attributes-before":
				b.attrFailure = 1
			case "get":
				b.getFailure = errManagedCurrentRead
			case "body":
				b.bodyFailure = errManagedCurrentRead
			case "close":
				b.closeFailure = errManagedCurrentRead
			case "attributes-after":
				b.attrFailure = 2
			case "decode", "policy":
				wantIO = false
				data := []byte("not-json")
				if mode == "policy" {
					data = []byte(`{"policy":9}`)
				}
				if err := b.Bucket.Upload(context.Background(), m.key("checkpoint/CURRENT"), bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			got, exists, err := m.LoadManagedAppendOnlyLocator(context.Background(), sqlpolicy.ManagedGraphRequestMaterializationPolicy)
			if err == nil || exists || got != (ManagedAppendOnlyLocator{}) || wantIO && !errors.Is(err, errManagedCurrentRead) {
				t.Fatalf("read failure accepted or identity lost: mode=%s exists=%t err=%v", mode, exists, err)
			}
		})
	}
}
