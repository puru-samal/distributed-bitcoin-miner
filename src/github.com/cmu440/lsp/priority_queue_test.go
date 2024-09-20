package lsp

import (
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	// Test for inserting elements and retrieving the minimum
	pq := NewPQ()
	pq.Insert(5)
	pq.Insert(3)
	pq.Insert(8)
	pq.Insert(1)

	min, err := pq.GetMin()
	if err != nil {
		t.Fatalf("Error occurred: %v", err)
	}
	if min != 1 {
		t.Errorf("Expected min to be 1, but got %d", min)
	}

	// Test removing the minimum
	removedMin, err := pq.RemoveMin()
	if err != nil {
		t.Fatalf("Error occurred: %v", err)
	}
	if removedMin != 1 {
		t.Errorf("Expected removed min to be 1, but got %d", removedMin)
	}

	// Test after removal
	min, err = pq.GetMin()
	if err != nil {
		t.Fatalf("Error occurred: %v", err)
	}
	if min != 3 {
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
	pq.Insert(10)
	min, err = pq.GetMin()
	if err != nil {
		t.Fatalf("Error occurred: %v", err)
	}
	if min != 10 {
		t.Errorf("Expected min to be 10, but got %d", min)
	}
}

func TestEmptyPriorityQueue(t *testing.T) {
	// Test for an empty queue
	pq := NewPQ()

	_, err := pq.GetMin()
	if err == nil {
		t.Errorf("Expected error when calling GetMin on empty queue, but got none")
	}

	_, err = pq.RemoveMin()
	if err == nil {
		t.Errorf("Expected error when calling RemoveMin on empty queue, but got none")
	}
}

func TestMultipleInserts(t *testing.T) {
	// Test for multiple inserts
	pq := NewPQ()

	for i := 10; i >= 1; i-- {
		pq.Insert(i)
	}

	for i := 1; i <= 10; i++ {
		min, err := pq.GetMin()
		if err != nil {
			t.Fatalf("Error occurred: %v", err)
		}
		if min != i {
			t.Errorf("Expected min to be %d, but got %d", i, min)
		}

		_, _ = pq.RemoveMin()
	}
}
