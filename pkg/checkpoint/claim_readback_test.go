package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

type claimReadbackBucket struct {
	objstore.Bucket
	afterWrite func(context.Context, string) error
	match      string
	onGet      func(context.Context, string) error
	afterGet   bool
	nilVersion bool
	gets       int
}

func (b *claimReadbackBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets++
	if b.onGet != nil && !b.afterGet {
		if err := b.onGet(ctx, name); err != nil {
			return nil, err
		}
	}
	r, err := b.Bucket.Get(ctx, name)
	if err == nil && b.onGet != nil && b.afterGet {
		if err := b.onGet(ctx, name); err != nil {
			r.Close()
			return nil, err
		}
	}
	return r, err
}

func (b *claimReadbackBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	a, err := b.Bucket.Attributes(ctx, name)
	if b.nilVersion {
		a.Version = nil
	}
	return a, err
}

func (b *claimReadbackBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if err := b.Bucket.Upload(ctx, name, r, options...); err != nil {
		return err
	}
	if b.afterWrite != nil && (strings.HasSuffix(name, "/PUBLISHER") && b.match == "" || b.match != "" && strings.Contains(name, b.match)) {
		hook := b.afterWrite
		b.afterWrite = nil
		return hook(ctx, name)
	}
	return nil
}

func TestPublisherReadbackRejectsSuccessor(t *testing.T) {
	for _, operation := range []string{"acquire", "generation", "maintenance", "bind", "renew"} {
		for _, sameOwner := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sameOwner=%v", operation, sameOwner), func(t *testing.T) {
				ctx := context.Background()
				bucket := &claimReadbackBucket{Bucket: objstore.NewInMemBucket()}
				manager := NewManager(bucket, "readback", "", 1)
				var original *PublisherClaim
				if operation == "bind" || operation == "renew" {
					var err error
					original, err = manager.AcquirePublisherClaim(ctx, "original", 0, time.Minute)
					if err != nil {
						t.Fatal(err)
					}
				}
				bucket.afterWrite = func(ctx context.Context, name string) error {
					r, err := bucket.Bucket.Get(ctx, name)
					if err != nil {
						return err
					}
					var successor PublisherClaim
					err = json.NewDecoder(r).Decode(&successor)
					r.Close()
					if err != nil {
						return err
					}
					// Model lease expiry and takeover while the first writer is paused.
					if !sameOwner {
						successor.OwnerID = "successor"
					}
					successor.Generation++
					successor.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
					data, err := json.Marshal(successor)
					if err != nil {
						return err
					}
					return bucket.Bucket.Upload(ctx, name, bytes.NewReader(data))
				}
				var claim *PublisherClaim
				var err error
				switch operation {
				case "acquire":
					claim, err = manager.AcquirePublisherClaim(ctx, "original", 0, time.Minute)
				case "generation":
					claim, err = manager.AcquireGenerationClaim(ctx, "original", 1, time.Minute)
				case "maintenance":
					claim, err = manager.acquireMaintenanceClaim(ctx, "original", time.Minute)
				case "bind":
					claim, err = manager.BindPublisherClaim(ctx, original, 1, [32]byte{1}, time.Minute)
				case "renew":
					claim, err = manager.RenewPublisherClaim(ctx, original, time.Minute)
				}
				if !errors.Is(err, ErrPublisherFenced) || claim != nil {
					t.Fatalf("successor adopted: claim=%+v err=%v", claim, err)
				}
				if original != nil {
					before, err := manager.readPublisherClaim(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if err := manager.ReleasePublisherClaim(ctx, original); !errors.Is(err, ErrPublisherFenced) {
						t.Fatalf("stale release=%v", err)
					}
					after, err := manager.readPublisherClaim(ctx)
					if err != nil || *before.version != *after.version {
						t.Fatalf("successor changed: before=%+v after=%+v err=%v", before, after, err)
					}
				}
			})
		}
	}
}

func TestRecoveryPinReadbackRejectsSuccessor(t *testing.T) {
	ctx := context.Background()
	bucket := &claimReadbackBucket{Bucket: objstore.NewInMemBucket(), match: "/checkpoint/recovery-pins/"}
	manager := NewManager(bucket, "pin-readback", "", 1)
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "pin")}, 1)
	bucket.afterWrite = func(ctx context.Context, name string) error {
		r, err := bucket.Bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		var successor recoveryPinRecord
		err = json.NewDecoder(r).Decode(&successor)
		r.Close()
		if err != nil {
			return err
		}
		successor.Token = "successor-token"
		successor.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
		data, err := json.Marshal(successor)
		if err != nil {
			return err
		}
		return bucket.Bucket.Upload(ctx, name, bytes.NewReader(data))
	}
	pin, err := manager.PinRecoveryRoot(ctx, root, "owner", time.Minute)
	if pin != nil || !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("successor pin adopted: pin=%+v err=%v", pin, err)
	}
}

func TestStableLeaseObjectPairsBodyAndVersion(t *testing.T) {
	for _, mode := range []string{"before-get", "after-get", "churn", "nil-version"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			b := &claimReadbackBucket{Bucket: objstore.NewInMemBucket(), afterGet: mode == "after-get", nilVersion: mode == "nil-version"}
			m := NewManager(b, "stable", "", 1)
			if err := b.Bucket.Upload(ctx, "key", strings.NewReader("old")); err != nil {
				t.Fatal(err)
			}
			b.onGet = func(ctx context.Context, name string) error {
				if mode != "churn" {
					b.onGet = nil
				}
				return b.Bucket.Upload(ctx, name, strings.NewReader(fmt.Sprintf("new-%d", b.gets)))
			}
			data, version, err := m.readStableLeaseObject(ctx, "key", 128)
			if mode == "churn" || mode == "nil-version" {
				if err == nil || data != nil || version != nil || b.gets != 4 {
					t.Fatalf("data=%q version=%v gets=%d err=%v", data, version, b.gets, err)
				}
				return
			}
			a, attrErr := b.Bucket.Attributes(ctx, "key")
			if err != nil || attrErr != nil || string(data) != "new-1" || version == nil || *version != *a.Version || b.gets != 2 {
				t.Fatalf("data=%q version=%v current=%v gets=%d err=%v/%v", data, version, a.Version, b.gets, err, attrErr)
			}
		})
	}
}

func TestPublisherConfirmationRequiresExactLivePayload(t *testing.T) {
	ctx := context.Background()
	m := NewManager(objstore.NewInMemBucket(), "confirm", "", 1)
	base := PublisherClaim{ConfigID: 1, Generation: 1, OwnerID: "owner", Purpose: "publisher", ReservedIndex: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	for _, field := range []string{"healthy", "purpose", "reservation", "binding", "root", "deadline", "expired"} {
		t.Run(field, func(t *testing.T) {
			stored, expected := base, base
			switch field {
			case "purpose":
				stored.Purpose = "generation"
			case "reservation":
				stored.ReservedIndex++
			case "binding":
				stored.BoundIndex++
			case "root":
				stored.RootHash = "other"
			case "deadline":
				stored.LeaseUntilMS++
			case "expired":
				stored.LeaseUntilMS = 1
				expected.LeaseUntilMS = 1
			}
			if err := m.uploadPublisherClaim(ctx, m.key("checkpoint/PUBLISHER"), &stored); err != nil {
				t.Fatal(err)
			}
			got, err := m.confirmPublisherClaim(ctx, expected)
			if field == "healthy" {
				if err != nil || got == nil {
					t.Fatalf("%+v %v", got, err)
				}
				return
			}
			if got != nil || !errors.Is(err, ErrPublisherFenced) {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
}

func TestRecoveryPinConfirmationRejectsIdenticalExpiredPayload(t *testing.T) {
	ctx := context.Background()
	m := NewManager(objstore.NewInMemBucket(), "expired", "", 1)
	root := createFiles(t, m, ctx, []Source{source(t, RoleSQLite, "pin")}, 1)
	pin, err := m.PinRecoveryRoot(ctx, root, "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	expected := pin.record
	expected.LeaseUntilMS = 1
	if err := m.uploadRecoveryPin(ctx, pin.key, expected); err != nil {
		t.Fatal(err)
	}
	got, err := m.confirmRecoveryPin(ctx, pin.key, expected)
	if got != nil || !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestPublisherOldLeaseHandleRemainsUsable(t *testing.T) {
	ctx := context.Background()
	m := NewManager(objstore.NewInMemBucket(), "old-handle", "", 1)
	claim, err := m.AcquirePublisherClaim(ctx, "owner", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before, err := m.bucket.Attributes(ctx, m.key("checkpoint/PUBLISHER"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.RenewPublisherClaim(ctx, claim, 0); !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("zero renewal=%v", err)
	}
	if _, err := m.BindPublisherClaim(ctx, claim, 1, [32]byte{1}, 0); !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("zero bind=%v", err)
	}
	after, err := m.bucket.Attributes(ctx, m.key("checkpoint/PUBLISHER"))
	if err != nil || *before.Version != *after.Version {
		t.Fatalf("invalid lease changed claim: %v", err)
	}
	for _, lease := range []time.Duration{2 * time.Minute, 3 * time.Minute} {
		if _, err := m.RenewPublisherClaim(ctx, claim, lease); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.BindPublisherClaim(ctx, claim, 1, [32]byte{1}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := m.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatalf("pre-bind handle release=%v", err)
	}
}
