// Contains the common functionality (priority queue) for both the client and server

package lsp

import (
	"fmt"
)

// list of messages inside the priority queue
type priorityQueue struct {
	q []*Message
}

/** API **/

// create a new priority queue
func NewPQ() *priorityQueue {
	newQueue := &priorityQueue{
		q: make([]*Message, 0),
	}
	return newQueue
}

// insert a new message into the priority queue
// and maintain the min heap property
func (pq *priorityQueue) Insert(elem *Message) {
	pq.q = append(pq.q, elem)
	pq.minHeapifyUp(len(pq.q) - 1)
}

// get the message with the minimum sequence number
// if the priority queue is empty, return an error
func (pq *priorityQueue) GetMin() (*Message, error) {
	if len(pq.q) == 0 {
		return nil, fmt.Errorf("priority queue is empty")
	}

	return pq.q[0], nil
}

// remove the message with the minimum sequence number
// and maintain the minheap property
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

// check if the priority queue is empty
func (pq *priorityQueue) Empty() bool {
	return len(pq.q) == 0
}

// return the size of the priority queue
func (pq *priorityQueue) Size() int {
	return len(pq.q)
}

/** internal helpers **/

// check if the index is valid in the priority queue
func (pq *priorityQueue) isValidIdx(idx int) bool {
	return (0 <= idx) && (idx < len(pq.q))
}

// get the parent of the current index
func (pq *priorityQueue) parent(idx int) int {
	newIdx := (idx - 1) >> 1
	return newIdx
}

// get the left child of the current index
func (pq *priorityQueue) leftChild(idx int) int {
	newIdx := (idx << 1) + 1
	return newIdx
}

// get the right child of the current index
func (pq *priorityQueue) rightChild(idx int) int {
	newIdx := (idx << 1) + 2
	return newIdx
}

// maintain the min heap property by moving the element down
// used when removing the minimum element
func (pq *priorityQueue) minHeapifyDown(idx int) {
	if !pq.isValidIdx(idx) {
		return
	}
	lch := pq.leftChild(idx)
	rch := pq.rightChild(idx)
	minIdx := idx
	if pq.isValidIdx(lch) && pq.q[minIdx].SeqNum > pq.q[lch].SeqNum {
		minIdx = lch
	}
	if pq.isValidIdx(rch) && pq.q[minIdx].SeqNum > pq.q[rch].SeqNum {
		minIdx = rch
	}
	if minIdx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[minIdx]
		pq.q[minIdx] = tmp
		pq.minHeapifyDown(minIdx)
	}
}

// maintain the min heap property by moving the element up
// used when inserting a new element
func (pq *priorityQueue) minHeapifyUp(idx int) {
	if !pq.isValidIdx(idx) {
		return
	}
	p := pq.parent(idx)
	maxIdx := idx
	if pq.isValidIdx(p) && pq.q[maxIdx].SeqNum < pq.q[p].SeqNum {
		maxIdx = p
	}
	if maxIdx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[maxIdx]
		pq.q[maxIdx] = tmp
		pq.minHeapifyUp(maxIdx)
	}
}
