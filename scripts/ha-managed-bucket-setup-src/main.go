package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

var bucketPattern = regexp.MustCompile(`^rhiza-managed-[0-9]+-[0-9]+-[0-9a-f]{12}$`)

func validate(endpoint, bucket string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Port() == "" {
		return errors.New("explicit loopback HTTP origin required")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || len(bucket) > 63 || !bucketPattern.MatchString(bucket) {
		return errors.New("loopback IP and run-scoped test bucket required")
	}
	return nil
}

func run(args []string) error {
	if len(args) > 0 && (args[0] == "oracle-gated" || args[0] == "oracle-plan") {
		return runOracle(args)
	}
	if len(args) == 2 && args[0] == "emulator-service-account" {
		return writeEmulatorServiceAccount(args[1])
	}
	provider := "s3"
	if len(args) == 3 {
		provider, args = args[0], args[1:]
	}
	if len(args) != 2 || (provider != "s3" && provider != "gcs" && provider != "azure") {
		return errors.New("expected optional s3/gcs/azure, endpoint and run-scoped bucket")
	}
	if err := validate(args[0], args[1]); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return createEmulatorBucket(ctx, provider, args[0], args[1])
}

func createEmulatorBucket(ctx context.Context, provider, endpoint, bucket string) error {
	// No environment credentials, config files, IMDS, proxy or redirects.
	httpClient := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect rejected") }}
	switch provider {
	case "gcs":
		previous, present := os.LookupEnv("STORAGE_EMULATOR_HOST")
		if err := os.Setenv("STORAGE_EMULATOR_HOST", endpoint); err != nil {
			return errors.New("set isolated GCS emulator route")
		}
		defer func() {
			if present {
				_ = os.Setenv("STORAGE_EMULATOR_HOST", previous)
			} else {
				_ = os.Unsetenv("STORAGE_EMULATOR_HOST")
			}
		}()
		client, err := storage.NewClient(ctx, option.WithEndpoint(endpoint+"/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(httpClient))
		if err != nil {
			return fmt.Errorf("construct isolated GCS client: %w", err)
		}
		defer client.Close()
		if err := client.Bucket(bucket).Create(ctx, "rhiza-local-test", nil); err != nil {
			return fmt.Errorf("create isolated GCS bucket: %w", err)
		}
		return nil
	case "azure":
		key := base64.StdEncoding.EncodeToString([]byte("rhiza-local-test-key-only-32bytes!"))
		connection := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=rhizatest;AccountKey=%s;BlobEndpoint=%s/rhizatest;", key, endpoint)
		client, err := azblob.NewClientFromConnectionString(connection, &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: httpClient}})
		if err != nil {
			return errors.New("construct isolated Azure client")
		}
		if _, err := client.CreateContainer(ctx, bucket, nil); err != nil {
			return fmt.Errorf("create isolated Azure container: %w", err)
		}
		return nil
	case "s3":
		client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
			Credentials: credentials.NewStaticCredentialsProvider("rhiza-local-test", "rhiza-local-test", ""),
			HTTPClient:  httpClient})
		if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			return fmt.Errorf("create isolated emulator bucket: %w", err)
		}
		return nil
	default:
		return errors.New("unsupported explicit emulator provider")
	}
}

func writeEmulatorServiceAccount(destination string) (result error) {
	dir := filepath.Dir(destination)
	info, err := os.Lstat(dir)
	if err != nil || !filepath.IsAbs(destination) || filepath.Base(destination) != "gcs-emulator.json" || !strings.HasPrefix(filepath.Base(dir), "ha-managed-private.") || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("owned private emulator credential destination required")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return errors.New("emulator credential directory owner differs")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return errors.New("generate disposable emulator credential")
	}
	data, err := json.Marshal(map[string]string{"type": "service_account", "project_id": "rhiza-local-test", "private_key_id": "rhiza-local-test", "client_email": "rhiza@rhiza-local-test.iam.gserviceaccount.com", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})), "token_uri": "http://floci:4588/token"})
	if err != nil {
		return errors.New("encode disposable emulator credential")
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("create exclusive emulator credential")
	}
	defer func() {
		result = errors.Join(result, file.Close())
		if result != nil {
			result = errors.Join(result, os.Remove(destination))
		}
	}()
	_, err = file.Write(data)
	if err != nil {
		return errors.New("write disposable emulator credential")
	}
	return file.Sync()
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
