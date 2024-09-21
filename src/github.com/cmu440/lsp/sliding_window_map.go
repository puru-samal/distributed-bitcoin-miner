package lsp

import "fmt"

type sWindowMap struct {
	mp      map[int]*Message
	LB      int // Lower bound (inclusive)
	UB      int // Upper bound (exclusive)
	maxSize int
}

/** API **/

func NewSWM(lb int, ub int, sz int) *sWindowMap {
	newMap := &sWindowMap{
		mp:      make(map[int]*Message),
		LB:      lb,
		UB:      ub,
		maxSize: sz,
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
	msg, exist := m.mp[sn]
	delete(m.mp, sn)
	if sn == m.LB {
		m.LB++
		m.UB++
	}
	return msg, exist
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
