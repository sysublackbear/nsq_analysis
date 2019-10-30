package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error      // 向文件中写入数据
	ReadChan() chan []byte // 返回一个读的Chan
	Close() error          // 关闭
	Delete() error         // 删除
	Depth() int64          // 返回未读消息的个数
	Empty() error          // 清空操作
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64      // 当前读文件的指针偏移量(从文件的某个位置开始读)
	writePos     int64      // 当前写文件的指针偏移量
	readFileNum  int64      // 当前读的文件编号
	writeFileNum int64      // 当前写的文件编号，用于创建文件名使用，文件编号每次新建一个文件会递增1
	depth        int64      // 写一条消息加1，读一条消息减1，可以理解成还未读完的消息数量

	sync.RWMutex

	// instantiation time metadata
	name            string        // 名称
	dataPath        string        // 文件的路径
	maxBytesPerFile int64         // 每个文件的最大字节数
	minMsgSize      int32         // 单条消息的最小字节数，默认是MsgIDLength + 8 + 2 = 26
	maxMsgSize      int32         // 单条消息的最大字节数，默认是1024 * 1024 + minMsgSize
	syncEvery       int64         // 定期落盘文件的读写次数阈值（默认2500）
	syncTimeout     time.Duration // 定期落盘文件的时间戳阈值（默认2s）
	exitFlag        int32
	needSync        bool          // 需要落盘文件

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64         // 下次需要读的指针偏移量
	nextReadFileNum int64         // 下次需要读的文件编号

	readFile  *os.File            // 当前正在读的文件,如果为nil则读取下一个文件编号的文件
	writeFile *os.File            // 当前正在写的文件,如果为nil则新建文件
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte          // 通过ReadChan()函数暴露出去

	// internal channels
	writeChan         chan []byte // 写通道
	writeResponseChan chan error  // 写之后返回的结果
	emptyChan         chan int    // 清空文件的通道
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	logf AppLogFunc  // 处理日志的函数
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	// 1.初始化diskQueue实例
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	// 2.从磁盘中恢复diskQueue的状态。diskQueue会定时将自己的状态备份到文件中
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	// 3.开启一个协程执行ioLoop函数,ioLoop函数是整个diskQueue最核心的方法
	// 作用是实现diskQueue的消息循环，定时刷新文件，读写操作功能。
	go d.ioLoop()
	return &d
}

// Depth returns the depth of the queue
// 返回队列的深度
func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	// 正在退出
	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data  // 塞入写队列
	return <-d.writeResponseChan  // 从写响应队列中返回
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)  // 不需要d.sync
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	// 打上标记
	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	// 关闭exit管道
	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan  // 等待exitSyncChan结果，确保ioLoop已经退出

	if d.readFile != nil {
		d.readFile.Close()  // 关闭readFile句柄
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()  // 关闭writeFile句柄
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1  // 塞入emptyChan
	return <-d.emptyResponseChan  // 等待响应
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())  // 删除meta文件
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	// 把未读的所有文件全删除掉
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
// 从文件中读取一条消息
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {  // 如果readFile为nil,则读取新的文件
		// 获取并打开新的文件
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {  // 如果readPos大于0，则设置读指针偏移量
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)  // 设置reader
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes  // 设置下次需要读的指针偏移量
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	if d.nextReadPos > d.maxBytesPerFile {  // 如果下次需要读的位置大于每个文件的最大值
		// 关闭正在读的文件,并置为nil, nextReadFileNum递增1, nextReadPos置为0
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++  // 读取下一个文件
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {  // 如果writeFile为nil
		curFileName := d.fileName(d.writeFileNum)  // 根据writeFileNum获取文件名称
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)  // 创建文件
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {  // 如果writePos大于0，则设置写的指针偏移量
			// 上次写到哪里了
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))  // 本次消息的长度

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {  // 校验消息长度
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()  // 重置writeBuf缓冲区
	// 先写长度，再写消息体
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)  // 将数据长度dataLen以二进制大端的形式写入到writeBuf中
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)  // 将data写入到writeBuf缓冲区中
	if err != nil {
		return err
	}

	// only write to the file once
	// 将buffer落地到文件（节省一次系统调用）
	_, err = d.writeFile.Write(d.writeBuf.Bytes())  // 将writeBuf缓冲区的数据写入到文件中
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes  // 改变writePos（写的偏移量）
	atomic.AddInt64(&d.depth, 1)

	// 写的位置超过了单个文件限制的最大值
	if d.writePos > d.maxBytesPerFile {
		/*
		1. 将writeFileNum递增1，用于创建下一个文件使用;
		2. writePos写的指针偏移量置为0;
		3. 刷新文件;
		4. 关闭文件;
		5. 将writeFile置为nil，那么下次有新消息需要写入文件则会新建文件;
		6. 这个操作的目的是为了防止单个文件过大
		 */
		d.writeFileNum++  // 向前移动一个文件
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()  // 持久化文件
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil  // 置为nil，等下次有新消息写入再重新创建
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()  // 写句柄Sync
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	// 元数据更新持久化
	err := d.persistMetaData()  // 持久化数据
	if err != nil {
		return err
	}

	d.needSync = false  // 已经Sync过，重置开关
	return nil
}

// retrieveMetaData initializes state from the filesystem
// 从meta文件中初始化队列信息
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()  // 找出队列的meta文件
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	// 从文件读取初depth(深度）,readFileNum, readPos, writeFileNum, writePos
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()  // meta文件名
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())  // 临时文件

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// 更新数据，当前读到了第几个文件，写到了第几个文件，以及深度
	// meta文件只是记了总的概要信息
	// d.depth——未读消息的个数
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// 纠错函数
func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			// 数据出现问题
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			// 数据丢失?
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)  // 将d.depth置为0
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			// d.readFileNum大于d.writeFileNum
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		// d.readPos比d.writePos要大
		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

// readOnce向前移动一条消息的长度
func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1) // todo:d.depth有什么含义

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}

	// 根据深度纠错
	d.checkTailCorruption(depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()  // 关闭写
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)

		// 有两种情况下会落盘文件：
		// 1.当count达到syncEvery时，即读写的次数累积到syncEvery。落盘文件后count会重置为0
		if count == d.syncEvery {  // count累加到d.syncEvery才进行落盘操作
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		// 判断到有可读的消息
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			// 读取文件中的下一条消息需要满足两个条件：
			// 1.文件中有可读的消息
			// 2.d.nextReadPos == d.readPos，即上次读取得到的消息已经投递出去，需要读取下条新的消息
			if d.nextReadPos == d.readPos {
				/*
				d.nextReadPos和d.readPos之间的区别：
				1. nextReadPos是下次要读取消息的偏移量，读取的消息会赋值给dataRead
				2. 当消息读取到之后不一定本次循环一定会执行(case r <- dataRead)，也可能会去执行其他的case分支。此时
				nextReadPos是下次要读的消息位置，而readPos是本次消息读的位置。
				3. nextReadPos是下次要读取消息的偏移量，也是消息投递成功后需要读的位置。而readPos当消息投递出去才会等于nextReadPos的值

				简单来说：
				1. 消息读取前: nextReadPos = readPos
				2. 消息已读取，但没有投递出去，nextReadPos是下次消息要读的位置，而readPos仍是本次消息读的开始位置。
				此时：nextReadPos =  readPos + 本条消息的长度
				3. 消息投递成功后： readPos  = nextReadPos, 将nextReadPos的值赋值给readPos
				 */
				dataRead, err = d.readOne()  // 读取一条消息
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()  // 处理读错误，略过这一文件
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// count的变化规则：
		// 1.如果一次消息循环中，有读或写操作，count会自增1
		// 2.当count达到syncEvery时，count会置为0，并刷新文件。
		// 3.当收到emptyChan的消息时，会将count会置为0，因为文件已经被删除了。

		// 如果r为空，则这个分支会被跳过。这个特性的使用了select的逻辑，简化了当数据为空时的判断
		// ！！！ 这个写法值得学习
		case r <- dataRead:
			count++
			// moveForward sets needSync flag if a file is removed
			d.moveForward()
		case <-d.emptyChan:  // 清空管道的消息（位于Empty函数当中）
			d.emptyResponseChan <- d.deleteAllFiles()  // 删除所有的队列文件
			count = 0
		case dataWrite := <-d.writeChan:  // 从写队列中取出消息（位于Put函数当中）
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		// 2.定时刷新，每隔syncTimeout就会刷新文件
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan:  // 退出
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
