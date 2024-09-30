package lsp

import (
	"fmt"
)

type priorityQueue struct {
	q []*Message
}

/** API **/

func NewPQ() *priorityQueue {
	newQueue := &priorityQueue{
		q: make([]*Message, 0),
	}
	return newQueue
}

func (pq *priorityQueue) Insert(elem *Message) {
	pq.q = append(pq.q, elem)
	pq.minHeapifyUp(len(pq.q) - 1)
}

func (pq *priorityQueue) GetMin() (*Message, error) {
	if len(pq.q) == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}

	return pq.q[0], nil
}

func (pq *priorityQueue) RemoveMin() (*Message, error) {
	min, err := pq.GetMin()

	if err != nil {
		return nil, err
	}

	pq.q[0] = pq.q[len(pq.q)-1]
	pq.q = pq.q[:len(pq.q)-1]
	pq.minHeapifyDown(0)
	return min, nil
}

func (pq *priorityQueue) Empty() bool {
	return len(pq.q) == 0
}

func (pq *priorityQueue) Size() int {
	return len(pq.q)
}

/** internal helpers **/

func (pq *priorityQueue) isValidIdx(idx int) bool {
	return (0 <= idx) && (idx < len(pq.q))
}

func (pq *priorityQueue) parent(idx int) int {
	new_idx := (idx - 1) >> 1
	return new_idx
}

func (pq *priorityQueue) leftChild(idx int) int {
	new_idx := (idx << 1) + 1
	return new_idx
}

func (pq *priorityQueue) rightChild(idx int) int {
	new_idx := (idx << 1) + 2
	return new_idx
}

func (pq *priorityQueue) minHeapifyDown(idx int) {
	if !pq.isValidIdx(idx) {
		return
	}
	lch := pq.leftChild(idx)
	rch := pq.rightChild(idx)
	min_idx := idx
	if pq.isValidIdx(lch) && pq.q[min_idx].SeqNum > pq.q[lch].SeqNum {
		min_idx = lch
	}
	if pq.isValidIdx(rch) && pq.q[min_idx].SeqNum > pq.q[rch].SeqNum {
		min_idx = rch
	}
	if min_idx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[min_idx]
		pq.q[min_idx] = tmp
		pq.minHeapifyDown(min_idx)
	}
}

func (pq *priorityQueue) minHeapifyUp(idx int) {
	if !pq.isValidIdx(idx) {
		return
	}
	p := pq.parent(idx)
	max_idx := idx
	if pq.isValidIdx(p) && pq.q[max_idx].SeqNum < pq.q[p].SeqNum {
		max_idx = p
	}
	if max_idx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[max_idx]
		pq.q[max_idx] = tmp
		pq.minHeapifyUp(max_idx)
	}
}

func (pq *priorityQueue) Remove(seqNum int) bool {
	index := -1
	newPQ := pq.q
	for i := range newPQ {
		if newPQ[i].SeqNum == seqNum {
			index = i
			break
		}
	}
	if index == -1 {
		return false
	}
	pq.q[index] = pq.q[len(pq.q)-1]
	pq.q = pq.q[:len(pq.q)-1]
	pq.minHeapifyDown(index)
	return true
}
