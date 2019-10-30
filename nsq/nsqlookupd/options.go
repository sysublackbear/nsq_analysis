package nsqlookupd

import (
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	// TCP客户端监听的<addr>:<port>
	TCPAddress       string `flag:"tcp-address"`
	// http-address: <addr>:<port> 监听 HTTP 客户端
	HTTPAddress      string `flag:"http-address"`
	// broadcast-address: 这个lookupd节点的外部地址,(默认是OS主机名)
	BroadcastAddress string `flag:"broadcast-address"`

	// inactive-producer-timeout: 从上次ping之后，生产者驻留在活跃列表中的时长
	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	// tombstone-lifetime: 生产者保持tombstoned的时长
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		LogPrefix:        "[nsqlookupd] ",
		LogLevel:         lg.INFO,
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
