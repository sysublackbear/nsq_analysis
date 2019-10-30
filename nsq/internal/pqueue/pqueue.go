package pqueue

import (
	"container/heap"
)

type Item struct {
	Value    interface{}  // 储存的消息内容
	Priority int64        // 优先级的时间点
	Index    int          // 在切片中的索引值
}

// this is a priority queue as implemented by a min heap
// ie. the 0th element is the *lowest* value
type PriorityQueue []*Item

func New(capacity int) PriorityQueue {
	return make(PriorityQueue, 0, capacity)
}

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i  // 修改index
	pq[j].Index = j
}

// 向队列中添加元素
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(PriorityQueue, n, c*2)  // 扩容
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item)
	item.Index = n
	(*pq)[n] = item
}

// 取出队列中优先级最高的元素（即Priority值最小，也就是根部元素）
func (pq *PriorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {  // 长度小于容量的一半 && 容量大于25，进行缩容
		npq := make(PriorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

// 判断根部的元素是否超过max,如果超过则返回nil，如果没有则返回并移除根部的元素
func (pq *PriorityQueue) PeekAndShift(max int64) (*Item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]

	// 优先级不能大于max
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)  // 取出堆顶元素，调整堆

	return item, 0
}


// ok
