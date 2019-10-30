package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string
	name      string
	ctx       *context

	backend BackendQueue

	memoryMsgChan chan *Message
	exitFlag      int32
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer
	paused         int32
	ephemeral      bool
	deleteCallback func(*Channel)
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	// 根据MessageID获取Message实体
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue  // 储存了延时消息和消息投递失败需要等待指定时间后重新投递的消息
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue  // 储存了正在投递但还没确认投递成功的消息
	inFlightMutex    sync.Mutex
}

// NewChannel creates a new instance of the Channel type and returns a pointer
// topicName: topic名字
// channelName: channel名字
// ctx: 函数上下文
// deleteCallback: 删除的回调函数
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {
	// 初始化Channel,初始化topicName,name,memoryMsgChan,clients,ctx以及删除的回调函数deleteCallback
	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		// 从context里面找出NSQD结构
		c.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}

	// 给e2eProcessingLatencyStream赋值，主要用于统计消息投递的延迟
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	// 创建了两个map：inFlightMessages和deferredMessages
	// 创建了两个queue：inFlightPQ和deferredPQ
	c.initPQ()

	// 初始化backend为diskqueue，磁盘存储的消息文件
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	// 通知nsq创建了Channel
	c.ctx.nsqd.Notify(c)

	return c
}

func (c *Channel) initPQ() {
	// MemQueueSize默认值为10000
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	// 初始化inFlightPQ和deferredPQ两条队列
	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	// 设置exitFlag开关
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)  // Delete和Close的区别是是否c.ctx.nsq.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	for _, client := range c.clients {
		client.Close()  // 遍历所有的客户端，进行关闭
	}
	c.RUnlock()

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()  //情况channel的队列信息
		return c.backend.Delete()  // todo: deletes the backend files?
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()  // 重新初始化优先队列信息
	for _, client := range c.clients {
		client.Empty()  // 调用每个Consumer的Empty方法
	}

	for {
		select {
		case <-c.memoryMsgChan:  // 排空channel
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)  // 把消息写到BackendQueue中
			if err != nil {
				// 写失败忽略
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	c.inFlightMutex.Lock()
	// 遍历inFlightMessages
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	// 遍历deferredMessages
	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	// memoryMsgChan的长度 + BackendQueue的Depth
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)  // paused开关值
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()  // 调用每个consumer的Pause和UnPause方法
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {  // 正在退出
		return errors.New("exiting")
	}
	err := c.put(m)  // 把消息塞到管道
	if err != nil {
		return err
	}
	// 消息体个数加1
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

func (c *Channel) put(m *Message) error {
	select {
	// memoryMsgChan内存队列默认缓冲是10000,如果memoryMsgChan已满，则写入到硬盘中
	case c.memoryMsgChan <- m:  // 把消息塞入管道
	default:
		b := bufferPoolGet()  // 从对象池取出bytes.Buffer结构
		err := writeMessageToBackend(b, m, c.backend)  // 写入磁盘文件
		bufferPoolPut(b)  // 归还对象
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}


// todo:设置定时消息
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)  // 消息个数加1
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	// 从InFlightMessages中删除该messageID的key-value
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	// deliveryTS：代表这个消息是什么时候发送的
	// 设置新的超时时间减去消息发送的时刻的时间差，不能大于c.ctx.nsqd.getOpts().MaxMsgTimeout
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	// 更新消息的优先级
	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)  // 把它塞入到InFlightMessage中
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)  // 投入正在发送的队列
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	// 从inFlightMessages中剔除该message
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	// 从堆中删除元素
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	// 将消息msg根据消息id从inFlightMessages和inFlightPQ队列中移除
	msg, err := c.popInFlightMessage(clientID, id)  // 先从InFlight的map中删除
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)  // 从InFlight堆中移除消息
	atomic.AddUint64(&c.requeueCount, 1)  // 重新入队消息数加一

	if timeout == 0 {
		// 如果timeout为0，则将该消息重新添加到memoryMsgChan或磁盘文件中，等待下次投递
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)  // 直接塞入管道中
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	// 放入延时队列
	// 如果timeout大于0，则将消息添加到deferredMessages 和 deferredPQ   队列中等待重新投递
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// 添加订阅者
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return nil
	}

	// 不能超过最大值
	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}

	c.clients[clientID] = client
	return nil
}

// RemoveClient removes a client from the Channel's client list
// 移除订阅者
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	// 订阅者为空
	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })  // 只执行一次回调函数
	}
}

// 发送不延时消息
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now  // 立马发送
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// 发送延时消息
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}  // 初始化item,pri的值为当前时间+延时时间的时间戳
	err := c.pushDeferredMessage(item)  // 将消息存到map deferredMessages中
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)  // 将item添加到deferredPQ优先级队列中
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	// 核对clientID是否一致
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)  // 删除这个key
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()  // 消息早就从移除了
		return
	}
	c.inFlightPQ.Remove(msg.index)  // 从堆中删除元素
	c.inFlightMutex.Unlock()
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID  // 取出id
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()  //消息以及存在了
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)  // 塞到deferredPQ堆中
	c.deferredMutex.Unlock()
}

func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)  // 取出优先级不大于t的元素（堆顶为最小优先级）
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)  // 从DeferredQueue中取出消息塞入到memoryMsgChan中
	}

exit:
	return dirty
}

func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}

		// 超时消息个数加一
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()  // 调用consumer的TimedOutMessage接口
		}
		c.put(msg)
	}

exit:
	return dirty
}
