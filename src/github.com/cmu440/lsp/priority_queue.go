package lsp

import "fmt"

type priorityQueue struct {
	q []int
}

func NewPQ() *priorityQueue {
	newQueue := &priorityQueue{
		q: make([]int, 0),
	}
	return newQueue
}

func (pq *priorityQueue) isValidIdx(idx int) bool {
	return (0 <= idx) && (idx < len(pq.q))
}

func (pq *priorityQueue) Parent(idx int) int {
	new_idx := (idx - 1) >> 1
	return new_idx
}

func (pq *priorityQueue) LeftChild(idx int) int {
	new_idx := (idx << 1) + 1
	return new_idx
}

func (pq *priorityQueue) RightChild(idx int) int {
	new_idx := (idx << 1) + 2
	return new_idx
}

func (pq *priorityQueue) MinHeapifyDown(idx int) {

	if !pq.isValidIdx(idx) {
		return
	}

	leftChild := pq.LeftChild(idx)
	rightChild := pq.RightChild(idx)
	min_idx := idx

	if pq.isValidIdx(leftChild) && pq.q[min_idx] > pq.q[leftChild] {
		min_idx = leftChild
	}

	if pq.isValidIdx(rightChild) && pq.q[min_idx] > pq.q[rightChild] {
		min_idx = rightChild
	}

	if min_idx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[min_idx]
		pq.q[min_idx] = tmp
		pq.MinHeapifyDown(min_idx)
	}

}

func (pq *priorityQueue) MinHeapifyUp(idx int) {

	if !pq.isValidIdx(idx) {
		return
	}

	parent := pq.Parent(idx)
	max_idx := idx

	if pq.isValidIdx(parent) && pq.q[max_idx] < pq.q[parent] {
		max_idx = parent
	}

	if max_idx != idx {
		tmp := pq.q[idx]
		pq.q[idx] = pq.q[max_idx]
		pq.q[max_idx] = tmp
		pq.MinHeapifyUp(max_idx)
	}

}

func (pq *priorityQueue) Insert(elem int) {
	pq.q = append(pq.q, elem)
	pq.MinHeapifyUp(len(pq.q) - 1)
}

func (pq *priorityQueue) GetMin() (int, error) {
	if len(pq.q) == 0 {
		return 0, fmt.Errorf("priority queue is empty")
	}
	return pq.q[0], nil
}

func (pq *priorityQueue) RemoveMin() (int, error) {

	min, err := pq.GetMin()
	if err != nil {
		return 0, err
	}

	pq.q[0] = pq.q[len(pq.q)-1]
	pq.q = pq.q[:len(pq.q)-1]
	pq.MinHeapifyDown(0)
	return min, nil

}
