package daramjwee

import "sync"

// promotionProbation implements 2-hit probation for lower-tier promotion.
// The first lower-tier hit for a key is served without promoting to the top
// tier, preventing one-hit wonders from polluting the hot tier. The second
// hit (and later hits) promote normally. The set is bounded by cap entries
// with FIFO eviction.
type promotionProbation struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string
	cap   int
}

func newPromotionProbation(cap int) *promotionProbation {
	return &promotionProbation{
		seen: make(map[string]struct{}, cap),
		cap:  cap,
	}
}

// observe records a lower-tier hit for key and reports whether promotion
// should proceed: true on the second and subsequent hits, false on the
// first hit.
func (p *promotionProbation) observe(key string) bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.seen[key]; ok {
		return true
	}
	if p.cap > 0 && len(p.order) >= p.cap {
		oldest := p.order[0]
		p.order = p.order[1:]
		delete(p.seen, oldest)
	}
	p.seen[key] = struct{}{}
	p.order = append(p.order, key)
	return false
}

// forget removes a key from the probation set, resetting its promotion
// state. Called when a key is deleted.
func (p *promotionProbation) forget(key string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.seen[key]; ok {
		delete(p.seen, key)
		for i, k := range p.order {
			if k == key {
				p.order = append(p.order[:i], p.order[i+1:]...)
				break
			}
		}
	}
}
