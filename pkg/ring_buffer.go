package pkg

import (
	"fmt"
	"sync"
)

type Record struct {
	Datetime  string `json:"datetime"`
	Value     string `json:"value"`
	Partition string `json:"partition"`
}

type RingBuffer struct {
	data     []Record
	addCh    chan Record
	getCh    chan Record
	stopCh   chan struct{}
	stopped  bool
	getEmpty bool // Track if Get channel is empty
}

func NewRingBuffer(size int) *RingBuffer {
	rb := &RingBuffer{
		data:     make([]Record, size),
		addCh:    make(chan Record, size),
		getCh:    make(chan Record),
		stopCh:   make(chan struct{}),
		getEmpty: true,
	}

	go rb.run()

	return rb
}

func (rb *RingBuffer) run() {
	start, end := 0, 0

	for {
		if start == end {
			select {
			case record := <-rb.addCh:
				rb.data[end] = record
				end = (end + 1) % len(rb.data)
				rb.getEmpty = false
			case <-rb.stopCh:
				rb.stopped = true
				return
			}
		} else {
			select {
			case record := <-rb.addCh:
				rb.data[end] = record
				end = (end + 1) % len(rb.data)
				rb.getEmpty = false
			case rb.getCh <- rb.data[start]:
				start = (start + 1) % len(rb.data)
				if start == end {
					rb.getEmpty = true
				}
			case <-rb.stopCh:
				rb.stopped = true
				return
			}
		}
	}
}

func (rb *RingBuffer) Add(record Record) {
	if rb.stopped {
		return
	}
	rb.addCh <- record
}

func (rb *RingBuffer) Get() (Record, bool) {
	if rb.stopped && rb.getEmpty {
		return Record{}, false
	}
	select {
	case record := <-rb.getCh:
		return record, true
	default:
		return Record{}, false
	}
}

func (rb *RingBuffer) Stop() {
	close(rb.stopCh)
}

func ProcessRecords(rb *RingBuffer, numReaders int) {
	var wg sync.WaitGroup
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				record, ok := rb.Get()
				if !ok {
					break
				}
				fmt.Printf("Reader %d: %+v\n", id, record) // Use %+v to print all fields of the record
			}
		}(i)
	}
	wg.Wait()
}
