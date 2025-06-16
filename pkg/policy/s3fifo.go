// Package policy provides implementations of the daramjwee.EvictionPolicy interface.
package policy

import (
	"container/list"

	"github.com/mrchypark/daramjwee"
)

// s3fifoEntry는 S3-FIFO 정책에서 캐시의 각 아이템을 나타냅니다.
type s3fifoEntry struct {
	key    string
	size   int64
	isMain bool // 아이템이 메인 큐에 있는지, 아니면 작은 큐에 있는지 여부
	wasHit bool // 메인 큐에 있는 동안 접근되었는지 (두 번째 기회를 받았는지)
}

// S3FIFOPolicy는 S3-FIFO 알고리즘의 핵심 아이디어를 차용한
// Second-Chance FIFO 교체 정책입니다.
// 이 구현은 스레드 안전하지 않으며, 외부(Store)에서 동기화를 관리해야 합니다.
type S3FIFOPolicy struct {
	smallQueue *list.List               // 새로 추가된 아이템을 위한 '작은' 큐
	mainQueue  *list.List               // 한 번 이상 접근된 아이템을 위한 '메인' 큐
	cache      map[string]*list.Element // 빠른 조회를 위한 맵

	// smallQueue와 mainQueue의 현재 크기 (바이트)
	smallSize int64
	mainSize  int64

	// smallQueue가 가질 수 있는 최대 크기. 나머지는 mainQueue가 사용합니다.
	smallCapacity int64
}

// NewS3FIFOPolicy는 새로운 S3-FIFO 정책을 생성합니다.
// totalCapacity는 캐시의 총 용량(바이트)이며,
// smallRatio는 전체 용량 중 smallQueue에 할당할 비율입니다 (예: 0.1은 10%).
func NewS3FIFOPolicy(totalCapacity int64, smallRatio float64) daramjwee.EvictionPolicy {
	if smallRatio <= 0 || smallRatio >= 1.0 {
		smallRatio = 0.1 // 기본값 10%
	}
	smallCap := int64(float64(totalCapacity) * smallRatio)
	return &S3FIFOPolicy{
		smallQueue:    list.New(),
		mainQueue:     list.New(),
		cache:         make(map[string]*list.Element),
		smallCapacity: smallCap,
	}
}

// 컴파일 타임에 EvictionPolicy 인터페이스를 만족하는지 확인합니다.
var _ daramjwee.EvictionPolicy = (*S3FIFOPolicy)(nil)

// Add는 새로운 아이템을 캐시에 추가합니다. 아이템은 항상 smallQueue로 먼저 들어갑니다.
func (p *S3FIFOPolicy) Add(key string, size int64) {
	if elem, ok := p.cache[key]; ok {
		// 이미 존재하는 아이템이면 사이즈만 업데이트합니다.
		entry := elem.Value.(*s3fifoEntry)
		if entry.isMain {
			p.mainSize -= entry.size
			p.mainSize += size
		} else {
			p.smallSize -= entry.size
			p.smallSize += size
		}
		entry.size = size
		return
	}

	// 새 아이템 추가
	entry := &s3fifoEntry{key: key, size: size, isMain: false, wasHit: false}
	elem := p.smallQueue.PushFront(entry)
	p.cache[key] = elem
	p.smallSize += size
}

// Touch는 아이템이 접근되었을 때 호출됩니다.
// smallQueue에 있던 아이템은 mainQueue로 승급(promote)됩니다.
// mainQueue에 있던 아이템은 'wasHit' 플래그가 true로 설정됩니다.
func (p *S3FIFOPolicy) Touch(key string) {
	elem, ok := p.cache[key]
	if !ok {
		return
	}

	entry := elem.Value.(*s3fifoEntry)

	if !entry.isMain {
		// Small -> Main 큐로 승급
		p.smallQueue.Remove(elem)
		p.smallSize -= entry.size

		entry.isMain = true
		newElem := p.mainQueue.PushFront(entry)
		p.cache[key] = newElem
		p.mainSize += entry.size
	} else {
		// Main 큐 내에서 접근된 경우, 히트 플래그 설정
		entry.wasHit = true
	}
}

// Remove는 캐시에서 아이템을 제거합니다.
func (p *S3FIFOPolicy) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict는 축출할 아이템의 키를 반환합니다.
// 먼저 smallQueue가 용량을 초과했는지 확인하고, 그렇지 않으면 mainQueue에서 축출 대상을 찾습니다.
func (p *S3FIFOPolicy) Evict() []string {
	// 1. Small 큐가 용량을 초과하면, 가장 오래된 아이템을 축출합니다.
	if p.smallSize > p.smallCapacity {
		elem := p.smallQueue.Back()
		if elem != nil {
			entry := p.removeElement(elem)
			return []string{entry.key}
		}
	}

	// 2. Main 큐에서 축출 대상을 찾습니다.
	// 뒤에서부터 스캔하면서 'wasHit'가 false인 첫 번째 아이템을 찾습니다.
	for elem := p.mainQueue.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*s3fifoEntry)
		if entry.wasHit {
			// 두 번째 기회를 줍니다. 히트 플래그를 내리고 큐의 앞으로 보냅니다.
			entry.wasHit = false
			p.mainQueue.MoveToFront(elem)
		} else {
			// 축출 대상 발견
			victim := p.removeElement(elem)
			return []string{victim.key}
		}
	}

	// Main 큐에 축출할 대상이 없으면 (예: 비어있거나 모든 아이템이 방금 기회를 얻음)
	// Small 큐에서 가장 오래된 아이템을 축출합니다.
	if p.smallQueue.Len() > 0 {
		elem := p.smallQueue.Back()
		entry := p.removeElement(elem)
		return []string{entry.key}
	}

	return nil
}

// removeElement는 내부 헬퍼 함수로, 주어진 list.Element를 제거하고 정리합니다.
func (p *S3FIFOPolicy) removeElement(e *list.Element) *s3fifoEntry {
	entry := e.Value.(*s3fifoEntry)
	if entry.isMain {
		p.mainQueue.Remove(e)
		p.mainSize -= entry.size
	} else {
		p.smallQueue.Remove(e)
		p.smallSize -= entry.size
	}
	delete(p.cache, entry.key)
	return entry
}
