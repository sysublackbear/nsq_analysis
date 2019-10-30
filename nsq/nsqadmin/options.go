package nsqadmin

import (
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	// http-address: 监听的IP和Port
	HTTPAddress string `flag:"http-address"`
	BasePath    string `flag:"base-path"`

	// graphite-url: graphite HTTP 地址
	GraphiteURL   string `flag:"graphite-url"`
	// proxy-graphite: Proxy HTTP requests to graphite
	ProxyGraphite bool   `flag:"proxy-graphite"`

	StatsdPrefix        string `flag:"statsd-prefix"`
	StatsdCounterFormat string `flag:"statsd-counter-format"`
	StatsdGaugeFormat   string `flag:"statsd-gauge-format"`

	StatsdInterval time.Duration `flag:"statsd-interval"`

	// lookupd-http-address: lookupd的http地址
	NSQLookupdHTTPAddresses []string `flag:"lookupd-http-address" cfg:"nsqlookupd_http_addresses"`
	// nsqd-http-address: nsqd的http地址
	NSQDHTTPAddresses       []string `flag:"nsqd-http-address" cfg:"nsqd_http_addresses"`

	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout"`

	HTTPClientTLSInsecureSkipVerify bool   `flag:"http-client-tls-insecure-skip-verify"`
	HTTPClientTLSRootCAFile         string `flag:"http-client-tls-root-ca-file"`
	HTTPClientTLSCert               string `flag:"http-client-tls-cert"`
	HTTPClientTLSKey                string `flag:"http-client-tls-key"`

	AllowConfigFromCIDR string `flag:"allow-config-from-cidr"`

	// notification-http-endpoint: HTTP 端点 (完全限定) ，管理动作将会发送到
	NotificationHTTPEndpoint string `flag:"notification-http-endpoint"`

	AclHttpHeader string   `flag:"acl-http-header"`
	AdminUsers    []string `flag:"admin-user" cfg:"admin_users"`
}

func NewOptions() *Options {
	return &Options{
		LogPrefix:                "[nsqadmin] ",
		LogLevel:                 lg.INFO,
		HTTPAddress:              "0.0.0.0:4171",
		BasePath:                 "/",
		StatsdPrefix:             "nsq.%s",
		StatsdCounterFormat:      "stats.counters.%s.count",
		StatsdGaugeFormat:        "stats.gauges.%s",
		StatsdInterval:           60 * time.Second,
		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,
		AllowConfigFromCIDR:      "127.0.0.1/8",
		AclHttpHeader:            "X-Forwarded-User",
		AdminUsers:               []string{},
	}
}
