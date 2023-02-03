package elastic

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sort"
	"strings"
)

const TLSMinVersionDefault = tls.VersionTLS12

func makeCertPool(certFiles []string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, certFile := range certFiles {
		pem, err := os.ReadFile(certFile)
		if err != nil {
			return nil, fmt.Errorf(
				"could not read certificate %q: %v", certFile, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf(
				"could not parse any PEM certificates %q: %v", certFile, err)
		}
	}
	return pool, nil
}

func loadCertificate(config *tls.Config, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf(
			"could not load keypair %s:%s: %v", certFile, keyFile, err)
	}

	config.Certificates = []tls.Certificate{cert}
	config.BuildNameToCertificate()
	return nil
}

// ParseTLSVersion returns a `uint16` by received version string key that represents tls version from crypto/tls.
// If version isn't supported ParseTLSVersion returns 0 with error
func ParseTLSVersion(version string) (uint16, error) {
	if v, ok := tlsVersionMap[version]; ok {
		return v, nil
	}

	var available []string
	for n := range tlsVersionMap {
		available = append(available, n)
	}
	sort.Strings(available)
	return 0, fmt.Errorf("unsupported version %q (available: %s)", version, strings.Join(available, ","))
}

func (cfg *config) TLSConfig() (*tls.Config, error) {

	// Support deprecated variable names
	if cfg.TLSCA == "" && cfg.SSLCA != "" {
		cfg.TLSCA = cfg.SSLCA
	}
	if cfg.TLSCert == "" && cfg.SSLCert != "" {
		cfg.TLSCert = cfg.SSLCert
	}
	if cfg.TLSKey == "" && cfg.SSLKey != "" {
		cfg.TLSKey = cfg.SSLKey
	}

	// This check returns a nil (aka, "use the default")
	// tls.Config if no field is set that would have an effect on
	// a TLS connection. That is, any of:
	//     * client certificate settings,
	//     * peer certificate authorities,
	//     * disabled security, or
	//     * an SNI server name.
	if cfg.TLSCA == "" && cfg.TLSKey == "" && cfg.TLSCert == "" && !cfg.InsecureSkipVerify && cfg.ServerName == "" {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		Renegotiation:      tls.RenegotiateNever,
	}

	if cfg.TLSCA != "" {
		pool, err := makeCertPool([]string{cfg.TLSCA})
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		err := loadCertificate(tlsConfig, cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, err
		}
	}

	// Explicitly and consistently set the minimal accepted version using the
	// defined default. We use this setting for both clients and servers
	// instead of relying on Golang's default that is different for clients
	// and servers and might change over time.
	tlsConfig.MinVersion = TLSMinVersionDefault
	if cfg.TLSMinVersion != "" {
		version, err := ParseTLSVersion(cfg.TLSMinVersion)
		if err != nil {
			return nil, fmt.Errorf("could not parse tls min version %q: %w", cfg.TLSMinVersion, err)
		}
		tlsConfig.MinVersion = version
	}

	if cfg.ServerName != "" {
		tlsConfig.ServerName = cfg.ServerName
	}

	return tlsConfig, nil

}
