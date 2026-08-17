package output

import "sync/atomic"

type Budget struct {
	limit uint64
	used  atomic.Uint64
}

func NewBudget(limit uint64) *Budget {
	return &Budget{limit: limit}
}

func (b *Budget) TryAcquire(size uint64) bool {
	for {
		used := b.used.Load()
		if size > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+size) {
			return true
		}
	}
}

func (b *Budget) Release(size uint64) {
	b.used.Add(^uint64(size - 1))
}

func (b *Budget) Used() uint64 {
	return b.used.Load()
}
