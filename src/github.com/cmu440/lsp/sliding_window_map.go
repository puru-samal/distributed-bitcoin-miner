// Contains the common functionality (sliding window) for both the client and server

package lsp

import (
	"fmt"
	"sort"
)

// each unAckedMsgs contains
// the message, current backoff, and the number of epochs not acked
type unAckedMsgs struct {
	msg            *Message
	currBackoff    int // current backoff
	unAckedCounter int // number of epochs not acked
}

// queue of unAcked messages
// keeps track of lower bound and upper bound of the sliding window
// and the maximum size of the sliding window
type sWindowMap struct {
	mp      map[int]*unAckedMsgs
	LB      int // Lower bound (inclusive)
	UB      int // Upper bound (exclusive)
	maxSize int
	ackdSNs []int // list of acknowledged sequence numbers
}

/** API **/

// create a new sliding window map
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

// if the message is valid and the sliding window is not full,
// put the message in the sliding window
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

// get the message with the given sequence number
func (m *sWindowMap) Get(sn int) (*Message, bool) {
	unAckedMsg, exist := m.mp[sn]
	if !exist {
		return nil, exist
	}

	return unAckedMsg.msg, exist
}

// remove the message with the given sequence number
// and update the lower bound of the sliding window
// and add the sequence number to the list of acknowledged sequence numbers
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

// reinitialize the sliding window map
func (m *sWindowMap) Reinit(lb int, ub int, sz int) {
	m.LB = lb
	m.UB = ub
	m.maxSize = sz
}

// get the lower bound of the sliding window
func (m *sWindowMap) MinKey() int {
	return m.LB
}

// true if the sliding window is empty
func (m *sWindowMap) Empty() bool {
	return len(m.mp) == 0
}

// true if the sequence number is in the sliding window
func (m *sWindowMap) In(key int) bool {
	_, exist := m.mp[key]
	return exist
}

// update the backoffs of the unAcked messages
func (m *sWindowMap) UpdateBackoffs(maxBackoff int) (*priorityQueue, bool) {
	if m.Empty() {
		return nil, false
	}

	pq := NewPQ()

	for _, v := range m.mp {
		// update counter,
		// if equal to next backoff, mark for resend, update backoffs
		if v.unAckedCounter >= v.currBackoff {
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

// return the elements inside the sliding window
func (m *sWindowMap) String() string {
	return fmt.Sprintf("[Lb:%d, Ub:%d Sz:%d mSz:%d]", m.LB, m.UB, len(m.mp), m.maxSize)
}

// if the sequence number is in the sliding window
// and the sliding window is over the max size
func (c *clientInfo) IsValidMessage(seqNum int) bool {
	windowBound := c.unAckedMsgs.isValidKey(seqNum)
	maxUnAckedBound := len(c.unAckedMsgs.mp) < c.unAckedMsgs.maxSize
	return windowBound && maxUnAckedBound
}

/** helpers **/

// true if the sequence number is in the sliding window
func (m *sWindowMap) isValidKey(sn int) bool {
	return m.LB <= sn && sn < m.UB
}

// true if the sliding window is full
func (m *sWindowMap) isFull() bool {
	return len(m.mp) == m.maxSize
}

// return the minimum of two integers
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// update the lower bound and the upper bound of the sliding window
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
