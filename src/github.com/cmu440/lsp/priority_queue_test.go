package lsp

import (
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	// Test for inserting elements and retrieving the minimum
	connID := 0
	dummyPayload := make([]byte, 3)
	pq := NewPQ()
	pq.Insert(NewData(connID, 5, len(dummyPayload), dummyPayload, 0))
	pq.Insert(NewData(connID, 3, len(dummyPayload), dummyPayload, 0))
	pq.Insert(NewData(connID, 8, len(dummyPayload), dummyPayload, 0))
	pq.Insert(NewData(connID, 1, len(dummyPayload), dummyPayload, 0))

	min, exist := pq.GetMin()
	if !exist {
		t.Fatalf("Error occurred")
	}
	if min.SeqNum != 1 {
		t.Errorf("Expected min to be 1, but got %d", min)
	}

	// Test removing the minimum
	removedMin, err := pq.RemoveMin()
	if err != nil {
		t.Fatalf("Error occurred: %v", err)
	}
	if removedMin.SeqNum != 1 {
		t.Errorf("Expected removed min to be 1, but got %d", removedMin)
	}

	// Test after removal
	min, exist = pq.GetMin()
	if !exist {
		t.Fatalf("Error occurred")
	}
	if min.SeqNum != 3 {
		t.Errorf("Expected min to be 3, but got %d", min)
	}

	// Test removing all elements
	_, _ = pq.RemoveMin()
	_, _ = pq.RemoveMin()
	_, _ = pq.RemoveMin()

	// Test removing from an empty queue
	_, err = pq.RemoveMin()
	if err == nil {
		t.Errorf("Expected error when removing from empty queue, but got none")
	}

	// Test inserting and removing from an empty queue
	pq.Insert(NewData(connID, 10, len(dummyPayload), dummyPayload, 0))
	min, exist = pq.GetMin()
	if !exist {
		t.Fatalf("Error occurred: %v", err)
	}
	if min.SeqNum != 10 {
		t.Errorf("Expected min to be 10, but got %d", min)
	}
}

func TestEmptyPriorityQueue(t *testing.T) {
	// Test for an empty queue
	pq := NewPQ()

	_, exist := pq.GetMin()
	if exist {
		t.Errorf("Expected error when calling GetMin on empty queue, but got none")
	}

	_, err := pq.RemoveMin()
	if err == nil {
		t.Errorf("Expected error when calling RemoveMin on empty queue, but got none")
	}
}

func TestMultipleInserts(t *testing.T) {
	// Test for multiple inserts
	pq := NewPQ()

	for i := 10; i >= 1; i-- {
		connID := 0
		dummyPayload := make([]byte, 3)
		msg := NewData(connID, i, len(dummyPayload), dummyPayload, 0)
		pq.Insert(msg)
	}

	for i := 1; i <= 10; i++ {
		min, exist := pq.GetMin()
		if !exist {
			t.Fatalf("Error occurred.")
		}
		if min.SeqNum != i {
			t.Errorf("Expected min to be %d, but got %d", i, min)
		}

		_, _ = pq.RemoveMin()
	}
}
