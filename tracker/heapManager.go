package tracker

import (
	"container/heap"
	"context"
	"time"
)

func heapManager(ctx context.Context, latestCheckPoint uint64, incomingChan <-chan *checkPoint) {
	h := &checkPointHeap{}
	heap.Init(h)

	lastSentCheckPoint := latestCheckPoint
	for {
		select {
		case cp := <-incomingChan:
			heap.Push(h, cp)
			log.Info("HEAP MAANGER RECEIVEEEED", "num", cp.checkpoint, "digest", cp.digest)

		default:
			if h.Len() == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			top := (*h)[0]
			if !top.IsReady() { //|| top.checkpoint != nextExpected {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			//if top.checkpoint < lastSentCheckPoint+50 {
			//	time.Sleep(50 * time.Millisecond)
			//	continue
			//}

			heap.Pop(h)

			log.Info("sendToSovereign", "checkpoint", top.checkpoint)
			//sendToSovereign(top)
			//latestCheckPoint = top.checkpoint
			lastSentCheckPoint = top.checkpoint
			_ = lastSentCheckPoint
		case <-ctx.Done():
			return
		}
	}
}
