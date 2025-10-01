package tracker

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/block-vision/sui-go-sdk/models"
)

type CheckpointManager struct {
	mu                   sync.Mutex
	h                    checkPointHeap
	pending              map[uint64]*checkPoint // pentru acumulare events înainte de ready
	latestSentCheckpoint uint64
}

func NewCheckpointManager() *CheckpointManager {
	return &CheckpointManager{
		h:       checkPointHeap{},
		pending: make(map[uint64]*checkPoint),
	}
}

func (m *CheckpointManager) AddEvent(ev models.SuiEventResponse, ready bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, ok := m.pending[m.latestSentCheckpoint]
	if !ok {
		cp = &checkPoint{
			checkpoint: 0,
		}
		m.pending[m.latestSentCheckpoint] = cp
	}
	cp.events = append(cp.events, ev)
}

func (m *CheckpointManager) MarkCheckpointReady(cpId uint64) {
	cp, ok := m.pending[cpId]
	if !ok {
		return
	}

	cp.ready = true

	heap.Push(&m.h, cp)
	delete(m.pending, cpId)
}

func (m *CheckpointManager) AddCheckPoint(cp *checkPoint) {
	m.mu.Lock()
	defer m.mu.Unlock()

	heap.Push(&m.h, cp)
}

func (m *CheckpointManager) FlushWithDelta(lastSent uint64, delta uint64) []*checkPoint {
	var out []*checkPoint
	for m.h.Len() > 0 {
		top := m.h[0]
		if top.checkpoint <= lastSent {
			// deja trimis, ignor
			heap.Pop(&m.h)
			continue
		}
		if top.checkpoint > lastSent+delta {
			break // prea departe, așteptăm
		}
		heap.Pop(&m.h)
		out = append(out, top)
		lastSent = top.checkpoint
	}
	return out
}

func (m *CheckpointManager) heapManager(ctx context.Context, latestCheckPoint uint64, incomingChan <-chan *checkPoint) {
	h := &checkPointHeap{}
	heap.Init(h)

	lastSentCheckPoint := latestCheckPoint
	for {
		select {
		case cp := <-incomingChan:
			if cp.checkpoint != 0 {
				m.mu.Lock()
				m.latestSentCheckpoint = cp.checkpoint
				m.mu.Unlock()

				m.AddCheckPoint(cp)
				log.Info("HEAP MAANGER RECEIVEEEED FULL CHECKPOINT", "num", cp.checkpoint, "digest", cp.digest)
			} else {
				m.mu.Lock()
				m.pending[m.latestSentCheckpoint] = cp
				m.mu.Unlock()
			}

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
