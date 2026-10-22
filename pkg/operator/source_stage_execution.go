package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// sourceStagesFor derives staging identity from the authenticated, root-enrolled
// runtime. Configured canonical authority state always selects remote storage.
func (e *FencingExecutor) sourceStagesFor(ctx context.Context, runtime fencing.RuntimeBinding) (*SourceStageStore, error) {
	if ctx == nil || runtime.StatefulSetUID == "" || runtime.SourceClusterID == "" {
		return nil, fmt.Errorf("fencing: source staging requires an enrolled root identity")
	}
	if e != nil && e.AuthorityState != nil {
		evidence, err := e.authorityEvidenceFor(runtime)
		if err != nil {
			return nil, err
		}
		return NewRemoteSourceStageStore(evidence)
	}
	if e != nil && e.SourceStages != nil {
		return e.SourceStages, nil
	}
	return nil, fmt.Errorf("fencing: canonical source staging is not configured")
}

func (e *FencingExecutor) authorityEvidenceFor(runtime fencing.RuntimeBinding) (*AuthorityEvidenceStore, error) {
	if e == nil || e.AuthorityState == nil || runtime.StatefulSetUID == "" || runtime.SourceClusterID == "" {
		return nil, fmt.Errorf("fencing: canonical authority evidence root is not configured")
	}
	return NewAuthorityEvidenceStore(e.AuthorityState.bucket, e.AuthorityState.prefix, runtime.StatefulSetUID, runtime.SourceClusterID)
}

// openMutationPayload returns only the exact sealed bytes bound by the admitted
// intent. The source-stage spool is immutable after sealing and protected by its
// lifetime flock.
func (s *SourceStageStore) openMutationPayload(producerUID string, intent fencing.MutationIntent) (io.ReadCloser, error) {
	return s.openMutationPayloadContext(context.Background(), producerUID, intent)
}

func (s *SourceStageStore) openMutationPayloadContext(ctx context.Context, producerUID string, intent fencing.MutationIntent) (io.ReadCloser, error) {
	if s != nil && s.evidence != nil {
		return s.openRemoteMutationPayload(ctx, producerUID, intent)
	}
	if s == nil || producerUID == "" || intent.Kind != "upload" || !strings.HasPrefix(intent.PayloadRef, "payload-") {
		return nil, fmt.Errorf("fencing: mutation has no sealed staged payload")
	}
	ticket := strings.TrimPrefix(intent.PayloadRef, "payload-")
	if !validStageTicket(ticket) {
		return nil, fmt.Errorf("fencing: invalid mutation payload reference")
	}
	draft := intent
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.fatal != nil {
		return nil, fmt.Errorf("fencing: source staging store closed")
	}
	index := s.recordIndex(producerUID, ticket)
	if index < 0 || s.manifest.Records[index].Cancelled || s.manifest.Records[index].Draft != draft {
		return nil, fmt.Errorf("fencing: staged payload does not match admitted mutation")
	}
	record := s.manifest.Records[index]
	want := sourceStageDescriptor{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}
	if record.Payload == nil || *record.Payload != want {
		return nil, fmt.Errorf("fencing: original sealed payload descriptor differs")
	}
	if err := s.verify(want); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(s.dir, want.Ref), syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(s.dir, want.Ref))
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != want.Size || info.Mode().Perm()&0022 != 0 {
		_ = file.Close()
		return nil, fmt.Errorf("fencing: staged payload changed while opening")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil || hex.EncodeToString(hash.Sum(nil)) != want.Digest {
		_ = file.Close()
		return nil, fmt.Errorf("fencing: staged payload changed while reading")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
