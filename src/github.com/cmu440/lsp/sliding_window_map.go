package lsp

import (
	"fmt"
)

type sWindowMap struct {
	mp      map[int]*Message
	LB      int // Lower bound (inclusive)
	UB      int // Upper bound (exclusive)
	maxSize int
	pq      *priorityQueue
}

/** API **/

func NewSWM(lb int, ub int, sz int) *sWindowMap {
	newMap := &sWindowMap{
		mp:      make(map[int]*Message),
		LB:      lb,
		UB:      ub,
		maxSize: sz,
		pq:      nil,
	}
	return newMap
}

func (m *sWindowMap) Put(sn int, elem *Message) bool {
	if m.isValidKey(sn) && !m.isFull() {
		m.mp[sn] = elem
		return true
	}
	return false
}

func (m *sWindowMap) Get(sn int) (*Message, bool) {
	msg, exist := m.mp[sn]
	return msg, exist
}

func (m *sWindowMap) Remove(sn int) (*Message, bool) {

	msg, exist := m.Get(sn)
	if exist {
		delete(m.mp, sn)
		if sn == m.LB {
			m.LB++
			m.UB++
		}
		return msg, exist
	}
	return nil, exist
}

func (m *sWindowMap) Reinit(lb int, ub int, sz int) {
	m.LB = lb
	m.UB = ub
	m.maxSize = sz
}

func (m *sWindowMap) MinKey() int {
	return m.LB
}

func (m *sWindowMap) Empty() bool {
	return len(m.mp) == 0
}

func (m *sWindowMap) In(key int) bool {
	_, exist := m.mp[key]
	return exist
}

func (m *sWindowMap) GetMinMsg() (*Message, bool) {

	if len(m.mp) == 0 {
		return nil, false
	}

	minKey := m.UB
	for k := range m.mp {
		if minKey > k {
			minKey = k
		}
	}
	return m.mp[minKey], true
}

func (m *sWindowMap) PQify() bool {
	m.pq = NewPQ()
	for _, msg := range m.mp {
		m.pq.Insert(msg)
	}
	return (m.pq != nil) && (len(m.pq.q) == len(m.mp))
}

func (m *sWindowMap) String() string {
	return fmt.Sprintf("[Lb:%d, Ub:%d Sz:%d mSz:%d]", m.LB, m.UB, len(m.mp), m.maxSize)
}

/** helpers **/

func (m *sWindowMap) isValidKey(sn int) bool {
	return m.LB <= sn && sn < m.UB
}

func (m *sWindowMap) isFull() bool {
	return len(m.mp) == m.maxSize
}
