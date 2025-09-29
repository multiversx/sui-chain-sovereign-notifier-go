package tracker

import (
	"sync"

	"github.com/block-vision/sui-go-sdk/models"
)

type checkPoint struct {
	mu         sync.Mutex
	digest     string
	checkpoint uint64
	events     []models.SuiEventFilter
	ready      bool
}

// checkPointHeap este un min-heap după checkpoint (apoi digest pt. stabilitate)
type checkPointHeap []*checkPoint

func (h checkPointHeap) Len() int { return len(h) }
func (h checkPointHeap) Less(i, j int) bool {
	if h[i].checkpoint == h[j].checkpoint {
		return h[i].digest < h[j].digest // stabilitate
	}
	return h[i].checkpoint < h[j].checkpoint
}
func (h checkPointHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *checkPointHeap) Push(x interface{}) {
	*h = append(*h, x.(*checkPoint))
}
func (h *checkPointHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (cp *checkPoint) MarkReady(checkpoint uint64) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.checkpoint = checkpoint
	cp.ready = true
}

func (cp *checkPoint) IsReady() bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.ready
}
