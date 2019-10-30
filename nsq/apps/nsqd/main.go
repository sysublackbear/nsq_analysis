package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	// go-svc是个框架，可以自己实现service结构体，实现接口Service的Init，Start，Stop方法
	// 触发退出方法的信号有两个：SIGINT（输入任意键）和SIGTERM(kill)
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

// 服务初始化
func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

// 服务启动
func (p *program) Start() error {
	opts := nsqd.NewOptions()  // 创建默认的参数结构

	// 将用户配置的参数值设置到对应的参数(自定义参数配置覆盖默认配置）
	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)

	// 新建nsq对象，上面有检验Options配置信息是否有误的逻辑
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	// 加载元数据信息
	err = p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	// 将元数据持久化到文件
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	go func() {
		// 启动nsqd服务(主函数）
		err := p.nsqd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

// 服务关闭
func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
