package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

type managedTLSPaths struct {
	CertFile     string
	KeyFile      string
	ClientCAFile string
}

func (p managedTLSPaths) enabled() bool {
	return p.CertFile != "" || p.KeyFile != "" || p.ClientCAFile != ""
}

func (p managedTLSPaths) validate() error {
	if p.CertFile == "" && p.KeyFile == "" && p.ClientCAFile == "" {
		return nil
	}
	if p.CertFile == "" || p.KeyFile == "" {
		return errors.New("managed TLS requires both certificate and key files")
	}
	return nil
}

func newManagedServerTLSConfig(paths managedTLSPaths) (*tls.Config, error) {
	if err := paths.validate(); err != nil {
		return nil, err
	}
	if _, _, err := loadManagedServerTLS(paths); err != nil {
		return nil, err
	}
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			certificate, roots, err := loadManagedServerTLS(paths)
			if err != nil {
				return nil, err
			}
			config := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
			if roots != nil {
				config.ClientCAs = roots
				config.ClientAuth = tls.RequireAndVerifyClientCert
			} else {
				config.ClientAuth = tls.NoClientCert
			}
			return config, nil
		},
	}
	return config, nil
}

func loadManagedServerTLS(paths managedTLSPaths) (tls.Certificate, *x509.CertPool, error) {
	certificate, err := tls.LoadX509KeyPair(paths.CertFile, paths.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load managed TLS certificate: %w", err)
	}
	if paths.ClientCAFile == "" {
		return certificate, nil, nil
	}
	data, err := os.ReadFile(paths.ClientCAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load managed TLS client CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return tls.Certificate{}, nil, errors.New("managed TLS client CA contains no certificate")
	}
	return certificate, roots, nil
}

func managedReadyClient(paths managedTLSPaths, caFile, serverName string) (*http.Client, error) {
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if caFile == "" && !paths.enabled() {
		return client, nil
	}
	if caFile == "" {
		return nil, errors.New("managed TLS readiness requires a CA file")
	}
	if (paths.CertFile == "") != (paths.KeyFile == "") {
		return nil, errors.New("managed TLS client requires both certificate and key files")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if paths.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(paths.CertFile, paths.KeyFile)
		if err != nil {
			return nil, errors.New("invalid managed TLS client certificate")
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errors.New("managed TLS readiness CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("managed TLS readiness CA contains no certificate")
	}
	config.RootCAs = roots
	client.Transport = &http.Transport{TLSClientConfig: config}
	return client, nil
}
