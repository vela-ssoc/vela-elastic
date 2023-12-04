package elastic

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/proxy"
	"net"
	"net/http"
	"net/url"
	"time"
)

type config struct {
	Default             bool
	Proxy               bool
	Index               string
	Username            string
	Password            string
	TLSCA               string
	TLSCert             string
	TLSKey              string
	SSLKey              string
	SSLCA               string
	SSLCert             string
	ServerName          string
	InsecureSkipVerify  bool
	TLSMinVersion       string
	Timeout             int
	EnableSniffer       bool
	HealthCheckInterval int
	HealthCheckTimeout  int
	EnableGzip          bool
	AuthBearerToken     string
	URLs                []string
	Thread              int
	Interval            int
	Flush               int
	PageSize            int
}

func (cfg *config) name() string {
	return fmt.Sprintf("vela.elastic.%s", cfg.Index)
}

func (cfg *config) ProxyTransport() (*http.Transport, error) {
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return nil, err
	}

	tr := &http.Transport{
		TLSClientConfig: tlsCfg,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			p := proxy.New(fmt.Sprintf("%s://%s", network, addr))
			return p.Dail(ctx)
		},

		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			p := proxy.New(fmt.Sprintf("tls://%s", addr))
			return p.Dail(ctx)
		},
	}

	return tr, nil

}

func (cfg *config) Transport() (*http.Transport, error) {
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return nil, err
	}

	return &http.Transport{
		TLSClientConfig: tlsCfg,
	}, nil
}

func (cfg *config) OptionsFunc() ([]elastic.ClientOptionFunc, error) {
	var options []elastic.ClientOptionFunc

	var tr *http.Transport
	var err error
	if cfg.Proxy {
		tr, err = cfg.ProxyTransport()
	} else {
		tr, err = cfg.Transport()
	}

	if err != nil {
		return nil, err
	}

	httpclient := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(cfg.Timeout),
	}

	elasticURL, err := url.Parse(cfg.URLs[0])
	if err != nil {
		return nil, fmt.Errorf("parsing URL failed: %v", err)
	}

	options = append(options,
		elastic.SetHttpClient(httpclient),
		elastic.SetSniff(cfg.EnableSniffer),
		elastic.SetScheme(elasticURL.Scheme),
		elastic.SetURL(cfg.URLs...),
		elastic.SetHealthcheckInterval(time.Duration(cfg.HealthCheckInterval)),
		elastic.SetHealthcheckTimeout(time.Duration(cfg.HealthCheckTimeout)),
		elastic.SetGzip(cfg.EnableGzip),
	)

	if cfg.Username != "" && cfg.Password != "" {
		options = append(options,
			elastic.SetBasicAuth(cfg.Username, cfg.Password),
		)
	}

	if cfg.AuthBearerToken != "" {
		options = append(options,
			elastic.SetHeaders(http.Header{
				"Authorization": []string{fmt.Sprintf("Bearer %s", cfg.AuthBearerToken)},
			}),
		)
	}

	if time.Duration(cfg.HealthCheckInterval) == 0 {
		options = append(options,
			elastic.SetHealthcheck(false),
		)
	}

	return options, nil
}

func (cfg *config) NewIndex(L *lua.LState, key string, val lua.LValue) {
	switch key {
	case "index":
		cfg.Index = val.String()
	case "username":
		cfg.Username = val.String()
	case "password":
		cfg.Password = val.String()
	case "tls_ca":
		cfg.TLSCA = val.String()
	case "tls_cert":
		cfg.TLSCert = val.String()
	case "tls_key":
		cfg.TLSKey = val.String()
	case "tls_min_version":
		cfg.TLSMinVersion = val.String()
	case "ssl_ca":
		cfg.SSLCA = val.String()
	case "ssl_cert":
		cfg.SSLCert = val.String()
	case "ssl_key":
		cfg.SSLCA = val.String()
	case "server_name":
		cfg.ServerName = val.String()
	case "insecure_skip_verify":
		cfg.InsecureSkipVerify = lua.IsTrue(val)
	case "timeout":
		cfg.Timeout = lua.CheckInt(L, val)
	case "enable_sniffer":
		cfg.EnableSniffer = lua.CheckBool(L, val)
	case "enable_gzip":
		cfg.EnableGzip = lua.CheckBool(L, val)
	case "auth_bearer_token":
		cfg.AuthBearerToken = lua.CheckString(L, val)
	case "url":
		switch val.Type() {
		case lua.LTString:
			cfg.URLs = []string{val.String()}
		case lua.LTTable:
			cfg.URLs = auxlib.LTab2SS(val.(*lua.LTable))
		default:
			L.RaiseError("invalid URLs , got %s", val.Type().String())
		}

	case "thread":
		cfg.Thread = lua.CheckInt(L, val)

	case "interval":
		cfg.Interval = lua.CheckInt(L, val)

	case "flush":
		cfg.Flush = lua.CheckInt(L, val)

	case "proxy":
		cfg.Proxy = lua.CheckBool(L, val)

	case "page_size":
		n := lua.IsInt(val)
		if n < 100 {
			cfg.PageSize = 100
			return
		}
		cfg.PageSize = n
	}
}

func newConfig(L *lua.LState) *config {
	tab := L.CheckTable(1)
	cfg := &config{
		Thread:   3,
		Interval: 1,
		Flush:    10,
		PageSize: 500,
	}

	tab.Range(func(key string, val lua.LValue) {
		cfg.NewIndex(L, key, val)
	})

	return cfg
}
