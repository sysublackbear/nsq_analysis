package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	// basic options
	ID        int64       `flag:"node-id" cfg:"id"`
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	// tcp-address: TCP 客户端监听的<addr>:<port>
	TCPAddress               string        `flag:"tcp-address"`
	// http-address: 为HTTP客户端监听<addr>:<port>
	HTTPAddress              string        `flag:"http-address"`
	// https-address: 为HTTPS客户端监听<addr>:<port>
	HTTPSAddress             string        `flag:"https-address"`
	// broadcast-address: 通过lookupd注册的地址（默认名是 OS）
	BroadcastAddress         string        `flag:"broadcast-address"`
	// lookupd-tcp-address: 解析 TCP 地址名字 (可能会给多次)
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`
	// auth-http-address: <addr>:<port> 查询授权服务器 (可能会给多次)
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	// diskqueue options
	// data-path: 缓存消息的磁盘路径
	DataPath        string        `flag:"data-path"`
	// mem-queue-size: 内存里的消息数（单个topic里面的单个channel而言）
	MemQueueSize    int64         `flag:"mem-queue-size"`
	// max-bytes-per-file: 每个磁盘队列文件的字节数
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	// sync-every: 磁盘队列fsync的消息数
	SyncEvery       int64         `flag:"sync-every"`
	// sync-timeout: 每个磁盘队列fsync平均耗时
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int `flag:"queue-scan-selection-count"`
	QueueScanWorkerPoolMax   int `flag:"queue-scan-worker-pool-max"`
	QueueScanDirtyPercent    float64

	// msg and command options
	// msg-timeout: 自动重新队列消息前需要等待的时间
	MsgTimeout    time.Duration `flag:"msg-timeout"`
	// max-msg-timeout: 消息超时的最大时间间隔
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	// max-msg-size: 单个消息体的最大字节数
	MaxMsgSize    int64         `flag:"max-msg-size"`
	// max-body-size: 单个命令体的最大尺寸
	MaxBodySize   int64         `flag:"max-body-size"`
	// max-req-timeout: 消息重新排队的超时时间
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	// max-heartbeat-interval: 在客户端心跳间，最大的客户端配置时间间隔
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	// max-rdy-count: 客户端最大的RDY数量
	MaxRdyCount            int64         `flag:"max-rdy-count"`
	// max-output-buffer-size: 最大客户端输出缓存可配置大小(字节）
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	// max-output-buffer-timeout: 在flushing到客户端前，最长的配置时间间隔。
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout"`
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout"`
	MaxChannelConsumers    int           `flag:"max-channel-consumers"`

	// statsd integration
	// statsd-address: 统计进程的UDP<addr>:<port>
	StatsdAddress       string        `flag:"statsd-address"`
	// statsd-prefix: 发送给统计keys的前缀
	StatsdPrefix        string        `flag:"statsd-prefix"`
	StatsdInterval      time.Duration `flag:"statsd-interval"`
	// statsd-mem-stats: 切换发送内存和GC统计数据
	StatsdMemStats      bool          `flag:"statsd-mem-stats"`
	StatsdUDPPacketSize int           `flag:"statsd-udp-packet-size"`

	// e2e message latency
	// e2e-processing-latency-window-time: 计算这段时间里，点对点时间延迟（例如，60s 仅计算过去 60 秒）
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	// e2e-processing-latency-percentile: 消息处理时间的百分比（通过逗号可以多次指定，默认为 none）
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	// tls-cert: 证书文件路径
	TLSCert             string `flag:"tls-cert"`
	// tls-key: 私钥路径文件
	TLSKey              string `flag:"tls-key"`
	// tls-client-auth-policy: 客户端证书授权策略 ('require' or 'require-verify')
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	// tls-root-ca-file: 私钥证书授权PEM路径
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	// tls-required: 客户端连接需求TLS
	TLSRequired         int    `flag:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	// deflate: 运行协商压缩特性（客户端压缩）
	DeflateEnabled  bool `flag:"deflate"`
	// max-deflate-level: 最大的压缩比率等级
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	// snappy: 打开快速选项 (客户端压缩)
	SnappyEnabled   bool `flag:"snappy"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024, // 100mb
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,  // 1mb
		MaxBodySize:   5 * 1024 * 1024,  // 5mb
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,  // 64kb
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
