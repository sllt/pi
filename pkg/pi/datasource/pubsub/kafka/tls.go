package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

type TLSConfig struct {
	CertFile           string
	KeyFile            string
	CACertFile         string
	InsecureSkipVerify bool
}

func createTLSConfig(tlsConf *TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: tlsConf.InsecureSkipVerify, //nolint:gosec //Populate the value as per user input
	}

	if tlsConf.CACertFile != "" {
		caCert, err := os.ReadFile(tlsConf.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errCACertFileRead, err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("%w: invalid PEM certificate", errCACertFileRead)
		}
		tlsConfig.RootCAs = caCertPool
	}

	if (tlsConf.CertFile != "") != (tlsConf.KeyFile != "") {
		return nil, fmt.Errorf("%w: cert file and key file must be configured together", errClientCertLoad)
	}

	if tlsConf.CertFile != "" && tlsConf.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsConf.CertFile, tlsConf.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errClientCertLoad, err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func validateTLSConfigs(conf *Config) error {
	if (conf.TLS.CertFile != "") != (conf.TLS.KeyFile != "") {
		return fmt.Errorf("%w: cert file and key file must be configured together", errClientCertLoad)
	}

	protocol := strings.ToUpper(conf.SecurityProtocol)

	if protocol == protocolSSL || protocol == protocolSASLSSL {
		if conf.TLS.CACertFile == "" && !conf.TLS.InsecureSkipVerify && conf.TLS.CertFile == "" {
			return fmt.Errorf("for %s, provide either CA cert, client certs, or enable insecure mode: %w",
				protocol, errUnsupportedSecurityProtocol)
		}
	}

	return nil
}
