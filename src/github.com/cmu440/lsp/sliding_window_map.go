package lsp

import (
	"fmt"
	"sort"
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
	ackdSNs []int
}

/** API **/

func NewSWM(lb int, ub int, sz int) *sWindowMap {
	newMap := &sWindowMap{
		mp:      make(map[int]*unAckedMsgs),
		LB:      lb,
		UB:      ub,
		maxSize: sz,
		ackdSNs: make([]int, 0),
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
		m.ackdSNs = append(m.ackdSNs, sn)
		m.updateBound()
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
	pq := NewPQ()

	if m.Empty() {
		return pq, false
	}

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
		return pq, false
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

func (m *sWindowMap) updateBound() {
	if len(m.ackdSNs) == 0 {
		return
	}
	// Sort the array
	sort.Ints(m.ackdSNs)

	// If the first element is not the lower bound, return as is
	if m.ackdSNs[0] != m.LB {
		return
	}

	// Iterate over the array and update the lower bound
	var i int
	for i = 0; i < len(m.ackdSNs)-1; i++ {
		if m.ackdSNs[i+1]-m.ackdSNs[i] == 1 {
			m.LB++
			m.UB++
		} else {
			break
		}
	}

	// If all elements are consecutive, return updated lower bound and an empty slice
	if i == len(m.ackdSNs)-1 {
		m.LB++
		m.UB++
		m.ackdSNs = []int{}
		return
	}

	// Return the updated lower bound and the remaining slice
	m.LB++
	m.UB++
	m.ackdSNs = m.ackdSNs[i+1:]
}

func (c *clientInfo) isValidMessage(seqNum int) bool {
	windowBound := c.unAckedMsgs.isValidKey(seqNum)
	maxUnAckedBound := len(c.unAckedMsgs.mp) < c.unAckedMsgs.maxSize
	return windowBound && maxUnAckedBound
}
