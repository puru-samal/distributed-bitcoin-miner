package lsp

import (
	"fmt"
)

type unAckedMsgs struct {
	msg            *Message
	currBackoff    int
	unAckedCounter int
}

type sWindowMap struct {
	mp      map[int]*unAckedMsgs
	LB      int // Lower bound (inclusive)
	UB      int // Upper bound (exclusive)
	maxSize int
}

/** API **/

func NewSWM(lb int, ub int, sz int) *sWindowMap {
	newMap := &sWindowMap{
		mp:      make(map[int]*unAckedMsgs),
		LB:      lb,
		UB:      ub,
		maxSize: sz,
	}
	return newMap
}

func (m *sWindowMap) Put(sn int, elem *Message) bool {
	if m.isValidKey(sn) && !m.isFull() {
		m.mp[sn] = &unAckedMsgs{
			msg:            elem,
			currBackoff:    0,
			unAckedCounter: 0,
		}
		return true
	}
	return false
}

func (m *sWindowMap) Get(sn int) (*Message, bool) {
	unAckedMsg, exist := m.mp[sn]
	if !exist {
		return nil, exist
	}

	return unAckedMsg.msg, exist
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

	if m.Empty() {
		return nil, false
	}

	minKey := m.UB
	for k := range m.mp {
		if minKey > k {
			minKey = k
		}
	}
	return m.mp[minKey].msg, true
}

func (m *sWindowMap) UpdateBackoffs(maxBackoff int) (*priorityQueue, bool) {
	if m.Empty() {
		return nil, false
	}
	pq := NewPQ()
	for _, v := range m.mp {
		// update counter,
		// if equal to next backoff, mark for resend, update backoffs
		if v.unAckedCounter == v.currBackoff {
			pq.Insert(v.msg)
			if v.currBackoff == 0 {
				v.currBackoff++
			} else {
				v.currBackoff = 2 * v.currBackoff
			}
			v.currBackoff = min(v.currBackoff, maxBackoff)
			v.unAckedCounter = 0
		} else {
			v.unAckedCounter++
		}
	}
	if pq.Empty() {
		return nil, false
	}

	return pq, true
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

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
