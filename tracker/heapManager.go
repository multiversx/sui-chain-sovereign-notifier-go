package tracker

import (
	"container/heap"
	"context"
	"time"
)

func heapManager(ctx context.Context, incomingChan <-chan *checkPoint) {
	h := &checkPointHeap{}
	heap.Init(h)
	nextExpected := uint64(0)

	for {
		select {
		case cp := <-incomingChan:
			heap.Push(h, cp)

		default:
			if h.Len() == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			top := (*h)[0]
			if !top.ready || top.checkpoint != nextExpected {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			heap.Pop(h)
			//sendToSovereign(top)
			nextExpected++
		case <-ctx.Done():
			return
		}
	}
}
