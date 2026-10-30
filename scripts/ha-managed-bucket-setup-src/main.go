package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var bucketPattern = regexp.MustCompile(`^rhiza-managed-[0-9]+-[0-9]+-[0-9a-f]{12}$`)

func validate(endpoint, bucket string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Port() == "" {
		return errors.New("explicit loopback HTTP origin required")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || !bucketPattern.MatchString(bucket) {
		return errors.New("loopback IP and run-scoped test bucket required")
	}
	return nil
}

func run(args []string) error {
	if len(args) > 0 && (args[0] == "oracle-gated" || args[0] == "oracle-plan") {
		return runOracle(args)
	}
	if len(args) != 2 {
		return errors.New("expected endpoint and run-scoped bucket")
	}
	if err := validate(args[0], args[1]); err != nil {
		return err
	}
	// Construct directly: no environment credentials, config files, IMDS, or proxy.
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(args[0]), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("rhiza-local-test", "rhiza-local-test", ""),
		HTTPClient:  &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect rejected") }}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(args[1])}); err != nil {
		return fmt.Errorf("create isolated emulator bucket: %w", err)
	}
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
