// Package versityfixture starts a private, local Versity Gateway for opt-in
// integration measurements. It is intended for tests, not application use.
package versityfixture

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Server owns one Versity process, its lifetime supervisor, and temporary
// state. Raw access and child output evidence are written under EvidenceDir.
type Server struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Root           string
	LogPath        string
	OutputPath     string
	CompletionPath string
	EvidenceDir    string
	Version        string
	BinarySHA256   string
	RunID          string

	cmd           *exec.Cmd
	shutdownPipe  *os.File
	closePipeOnce sync.Once
	closePipeErr  error
	done          chan struct{}
	mu            sync.Mutex
	err           error
}

const childSupervisorScript = `
umask 077
output=$1
completion=$2
shift 2
child=
watcher=
status=0
forced=0
cleanup() {
	status=$?
	trap - EXIT
	trap '' HUP INT TERM USR1
	if [ -n "$child" ]; then
		if kill -INT "$child" 2>/dev/null; then
			i=0
			while [ "$i" -lt 20 ]; do
				state=$(ps -p "$child" -o stat= 2>/dev/null) || state=
				case "$state" in *Z*) break ;; esac
				[ -n "$state" ] || break
				sleep 0.1
				i=$((i + 1))
			done
			state=$(ps -p "$child" -o stat= 2>/dev/null) || state=
			case "$state" in
				'') ;;
				*Z*) ;;
				*)
					forced=1
					kill -KILL "$child" 2>/dev/null || :
					;;
			esac
		fi
		wait "$child"
		child_status=$?
		printf 'VERSITY_CHILD_REAPED status=%s forced=%s\n' "$child_status" "$forced" >>"$output"
		if [ "$forced" -ne 0 ]; then
			status=1
		elif [ "$child_status" -ne 0 ] && [ "$child_status" -ne 130 ]; then
			status=$child_status
		fi
	fi
	if [ -n "$watcher" ]; then
		kill -TERM "$watcher" 2>/dev/null || :
		wait "$watcher" 2>/dev/null || :
	fi
	tmp="$completion.$$"
	printf 'gateway_reaped=true\nchild_status=%s\nforced=%s\n' "$child_status" "$forced" >"$tmp"
	mv "$tmp" "$completion"
	exit "$status"
}
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 0' USR1
trap cleanup EXIT
(
	trap - INT QUIT
	exec "$@" 3<&- </dev/null >>"$output" 2>&1
) &
child=$!
wrapper_pid=$$
(
	while IFS= read -r ignored <&3; do :; done
	kill -USR1 "$wrapper_pid" 2>/dev/null || :
) &
watcher=$!
wait "$child"
child_status=$?
child=
printf 'VERSITY_GATEWAY_EXITED_BEFORE_PIPE_EOF status=%s\n' "$child_status" >>"$output"
if [ "$child_status" -eq 0 ]; then
	exit 1
fi
exit "$child_status"
`

// ClientCounts separates setup SDK transport attempts from responses. A
// request that never receives an HTTP response is not counted server-side.
type ClientCounts struct {
	attempts  atomic.Uint64
	responses atomic.Uint64
}

// Snapshot returns client transport attempts and attempts that got a response.
func (c *ClientCounts) Snapshot() (attempts, responses uint64) {
	return c.attempts.Load(), c.responses.Load()
}

// Start launches the provided pinned binary against an isolated POSIX root.
func Start(ctx context.Context, binary, parentDir string) (*Server, error) {
	info, err := os.Stat(binary)
	if err != nil || info.IsDir() || info.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("Versity binary must be executable: %q (%v)", binary, err)
	}
	versionCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	versionOut, err := exec.CommandContext(versionCtx, binary, "--version").Output()
	cancel()
	if err != nil {
		return nil, fmt.Errorf("read Versity version: %w", err)
	}
	binaryFile, err := os.Open(binary)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, binaryFile); err != nil {
		_ = binaryFile.Close()
		return nil, err
	}
	if err := binaryFile.Close(); err != nil {
		return nil, err
	}

	root, err := os.MkdirTemp(parentDir, "versity-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, err
	}
	dataDir, iamDir := filepath.Join(root, "data"), filepath.Join(root, "iam")
	for _, dir := range []string{dataDir, iamDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(nonce[:])
	evidenceBase := os.Getenv("RHIZA_VERSITY_EVIDENCE_DIR")
	if evidenceBase == "" {
		return nil, fmt.Errorf("RHIZA_VERSITY_EVIDENCE_DIR must name a persistent directory outside Go temp")
	}
	evidenceDir, err := prepareEvidenceDir(evidenceBase, parentDir, token)
	if err != nil {
		return nil, err
	}
	logPath, outputPath := filepath.Join(evidenceDir, "s3-access.log"), filepath.Join(evidenceDir, "server-output.log")
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	childArgs := []string{binary, "--port", addr, "--iam-dir", iamDir, "--access-log", logPath, "--quiet", "posix", dataDir}
	completionPath := filepath.Join(evidenceDir, "gateway-complete")
	cmd, shutdownPipe, err := startSupervisedChild(outputPath, completionPath, childArgs, []string{
		"ROOT_ACCESS_KEY=versity-" + token,
		"ROOT_SECRET_KEY=versity-secret-" + token,
		"PATH=/usr/bin:/bin",
	})
	if err != nil {
		_ = output.Close()
		return nil, fmt.Errorf("start Versity child supervisor: %w", err)
	}
	_ = output.Close()
	s := &Server{
		Endpoint: "http://" + addr, AccessKey: "versity-" + token,
		SecretKey: "versity-secret-" + token, Root: root, LogPath: logPath,
		OutputPath: outputPath, CompletionPath: completionPath, EvidenceDir: evidenceDir,
		Version:      strings.TrimSpace(string(versionOut)),
		BinarySHA256: hex.EncodeToString(hash.Sum(nil)), RunID: token, cmd: cmd,
		shutdownPipe: shutdownPipe, done: make(chan struct{}),
	}
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.done)
	}()
	if err := s.WaitReady(ctx); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
		defer cancel()
		if closeErr := s.Close(closeCtx); closeErr != nil {
			return nil, fmt.Errorf("%w (also failed to stop child: %v)", err, closeErr)
		}
		return nil, err
	}
	return s, nil
}

func startSupervisedChild(outputPath, completionPath string, args, env []string) (*exec.Cmd, *os.File, error) {
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("create child-lifetime pipe: %w", err)
	}
	cmdArgs := []string{"-c", childSupervisorScript, "rhiza-versity-supervisor", outputPath, completionPath}
	cmdArgs = append(cmdArgs, args...)
	cmd := exec.Command("sh", cmdArgs...)
	cmd.ExtraFiles = []*os.File{readPipe}
	cmd.Env = env
	cmd.Stdin = nil
	if err := cmd.Start(); err != nil {
		_ = readPipe.Close()
		_ = writePipe.Close()
		return nil, nil, err
	}
	if err := readPipe.Close(); err != nil {
		_ = writePipe.Close()
		_ = cmd.Wait()
		return nil, nil, fmt.Errorf("close parent copy of child-lifetime reader: %w", err)
	}
	return cmd, writePipe, nil
}

func prepareEvidenceDir(base, tempParent, runID string) (string, error) {
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("RHIZA_VERSITY_EVIDENCE_DIR must be absolute: %q", base)
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	absTemp, err := filepath.Abs(tempParent)
	if err != nil {
		return "", err
	}
	if pathContains(absTemp, absBase) || pathContains(absBase, absTemp) {
		return "", fmt.Errorf("RHIZA_VERSITY_EVIDENCE_DIR must be outside and disjoint from the Go temporary directory")
	}
	if err := os.MkdirAll(absBase, 0o700); err != nil {
		return "", fmt.Errorf("create Versity evidence directory: %w", err)
	}
	resolvedBase, err := filepath.EvalSymlinks(absBase)
	if err != nil {
		return "", fmt.Errorf("resolve Versity evidence directory: %w", err)
	}
	resolvedTemp, err := filepath.EvalSymlinks(tempParent)
	if err != nil {
		return "", fmt.Errorf("resolve Versity temporary directory: %w", err)
	}
	if pathContains(resolvedTemp, resolvedBase) || pathContains(resolvedBase, resolvedTemp) {
		return "", fmt.Errorf("RHIZA_VERSITY_EVIDENCE_DIR must be outside and disjoint from the Go temporary directory")
	}
	runDir := filepath.Join(resolvedBase, "versity-"+runID)
	if err := os.Mkdir(runDir, 0o700); err != nil {
		return "", fmt.Errorf("create fresh Versity evidence run directory: %w", err)
	}
	return runDir, nil
}

func pathContains(parent, candidate string) bool {
	rel, err := filepath.Rel(parent, candidate)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}

// WaitReady waits until this server's selected loopback port accepts TCP,
// while detecting early child exit. It emits no S3 request; callers should
// perform authenticated S3 readiness and count that separately.
func (s *Server) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	address := strings.TrimPrefix(s.Endpoint, "http://")
	for {
		select {
		case <-s.done:
			s.mu.Lock()
			err := s.err
			s.mu.Unlock()
			return fmt.Errorf("Versity child exited before readiness: %v", err)
		default:
		}
		dialer := net.Dialer{Timeout: 500 * time.Millisecond}
		conn, dialErr := dialer.DialContext(ctx, "tcp", address)
		if dialErr == nil {
			_ = conn.Close()
			select {
			case <-s.done:
				s.mu.Lock()
				childErr := s.err
				s.mu.Unlock()
				return fmt.Errorf("Versity child exited while opening listener: %v", childErr)
			default:
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Versity HTTP readiness: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// NewS3Client returns a MinIO setup client pinned to this server's generated
// credentials plus counters for setup traffic. It does not use ambient auth.
func (s *Server) NewS3Client() (*minio.Client, *ClientCounts, error) {
	counts := &ClientCounts{}
	client, err := minio.New(strings.TrimPrefix(s.Endpoint, "http://"), &minio.Options{
		Creds: credentials.NewStaticV4(s.AccessKey, s.SecretKey, ""), Secure: false,
		Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1,
		Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			counts.attempts.Add(1)
			response, err := http.DefaultTransport.RoundTrip(request)
			if response != nil {
				counts.responses.Add(1)
			}
			return response, err
		}),
	})
	return client, counts, err
}

// AccessRecords returns every nonempty raw access-log line, including server
// metadata headers, so callers can preserve the unmodified evidence.
func (s *Server) AccessRecords() ([]string, error) {
	f, err := os.Open(s.LogPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var records []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			records = append(records, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read Versity access log: %w", err)
	}
	return records, nil
}

// RequestCount counts records while excluding only Versity's documented log
// start metadata line. Other unrecognized lines are retained and counted.
func RequestCount(records []string) uint64 {
	var count uint64
	for _, record := range records {
		line := strings.TrimSpace(record)
		if line != "" && !strings.HasPrefix(line, "log starts ") {
			count++
		}
	}
	return count
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// Close signals the supervisor by closing its lifetime pipe and waits for the
// supervisor to stop and reap the child. The supervisor owns bounded cleanup,
// so a caller deadline never kills the supervisor while it owns the child.
func (s *Server) Close(ctx context.Context) error {
	if s.cmd == nil || s.cmd.Process == nil || s.shutdownPipe == nil {
		return nil
	}
	s.closePipeOnce.Do(func() { s.closePipeErr = s.shutdownPipe.Close() })
	select {
	case <-s.done:
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		if s.closePipeErr != nil {
			return fmt.Errorf("signal Versity child supervisor by closing lifetime pipe: %w", s.closePipeErr)
		}
		return err
	case <-ctx.Done():
		return fmt.Errorf("Versity child supervisor continues bounded cleanup after caller deadline: %w", ctx.Err())
	}
}
