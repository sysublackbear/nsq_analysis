package writers

import (
	"io"
	"time"
)

type SpreadWriter struct {
	w        io.Writer
	interval time.Duration
	buf      [][]byte
	exitCh   chan int
}

func NewSpreadWriter(w io.Writer, interval time.Duration, exitCh chan int) *SpreadWriter {
	return &SpreadWriter{
		w:        w,
		interval: interval,
		buf:      make([][]byte, 0),
		exitCh:   exitCh,
	}
}

// 所谓SpreadWriter，就是Write的时候先写到内存
func (s *SpreadWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	s.buf = append(s.buf, b)
	return len(p), nil
}

func (s *SpreadWriter) Flush() {
	sleep := s.interval / time.Duration(len(s.buf))
	ticker := time.NewTicker(sleep)
	for _, b := range s.buf {
		s.w.Write(b)
		select {
		case <-ticker.C:
		case <-s.exitCh: // skip sleeps finish writes
		}
	}
	ticker.Stop()  // 暂停计时器
	s.buf = s.buf[:0]
}
