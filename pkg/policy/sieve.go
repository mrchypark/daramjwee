// Filename: policy/sieve.go
package policy

import (
	"container/list"

	"github.com/mrchypark/daramjwee"
)

// sieveEntry는 Sieve 정책에서 캐시의 각 아이템을 나타냅니다.
type sieveEntry struct {
	key     string
	size    int64
	visited bool // 아이템이 최근에 접근되었는지 여부
}

// SievePolicy는 SIEVE 캐시 교체 알고리즘을 구현합니다.
// 이 구현은 스레드 안전하지 않으며, 외부(Store)에서 동기화를 관리해야 합니다.
//
// 핵심 아이디어:
// - 모든 아이템은 양방향 연결 리스트에 저장됩니다.
// - 'hand'라는 포인터가 리스트의 아이템을 가리킵니다.
// - 축출 시, 'hand'부터 리스트의 오래된 아이템 방향(뒤쪽)으로 스캔합니다.
// - 'visited' 플래그가 true인 아이템을 만나면, false로 바꾸고 기회를 한 번 더 줍니다. (살려둠)
// - 'visited' 플래그가 false인 아이템을 만나면, 해당 아이템을 축출합니다.
// - 아이템에 접근('Touch')하면 'visited' 플래그가 true가 됩니다.
type SievePolicy struct {
	ll    *list.List               // 아이템을 순서대로 저장하는 연결 리스트
	cache map[string]*list.Element // 빠른 조회를 위한 맵
	hand  *list.Element            // 축출 대상을 스캔하기 위한 '시계침(hand)' 포인터
}

// NewSievePolicy는 새로운 SIEVE 정책을 생성합니다.
func NewSievePolicy() daramjwee.EvictionPolicy {
	return &SievePolicy{
		ll:    list.New(),
		cache: make(map[string]*list.Element),
		hand:  nil, // 처음에는 hand가 설정되지 않음
	}
}

// 컴파일 타임에 EvictionPolicy 인터페이스를 만족하는지 확인합니다.
var _ daramjwee.EvictionPolicy = (*SievePolicy)(nil)

// Add는 새로운 아이템을 캐시에 추가합니다. 아이템은 리스트의 맨 앞에 추가됩니다.
func (p *SievePolicy) Add(key string, size int64) {
	if elem, ok := p.cache[key]; ok {
		// 이미 존재하는 아이템이면 사이즈를 업데이트하고 맨 앞으로 이동시킵니다.
		p.ll.MoveToFront(elem)
		entry := elem.Value.(*sieveEntry)
		entry.size = size
		// 접근으로 간주하여 visited 플래그를 설정할 수도 있으나,
		// 원본 SIEVE 논문에서는 Add 시에는 visited를 설정하지 않습니다.
		// 여기서는 LRU와 유사하게 처리하여 업데이트 시 최근 사용으로 간주합니다.
		entry.visited = true
		return
	}

	// 새 아이템 추가 (visited = false 상태로)
	entry := &sieveEntry{key: key, size: size, visited: false}
	elem := p.ll.PushFront(entry)
	p.cache[key] = elem
}

// Touch는 아이템이 접근되었을 때 호출됩니다. 해당 아이템의 visited 플래그를 true로 설정합니다.
func (p *SievePolicy) Touch(key string) {
	if elem, ok := p.cache[key]; ok {
		elem.Value.(*sieveEntry).visited = true
	}
}

// Remove는 캐시에서 아이템을 제거합니다.
func (p *SievePolicy) Remove(key string) {
	if elem, ok := p.cache[key]; ok {
		p.removeElement(elem)
	}
}

// Evict는 축출할 아이템을 결정하여 키를 반환합니다.
func (p *SievePolicy) Evict() []string {
	if p.ll.Len() == 0 {
		return nil
	}

	// hand가 가리키는 위치부터 스캔 시작
	victimElem := p.hand
	if victimElem == nil {
		// hand가 초기화되지 않았다면, 리스트의 맨 뒤(가장 오래된 아이템)에서 시작
		victimElem = p.ll.Back()
	}

	// visited가 false인 아이템을 찾을 때까지 스캔
	for {
		entry := victimElem.Value.(*sieveEntry)
		if entry.visited {
			// 기회를 한 번 줌: visited를 false로 바꾸고 다음 후보로 이동
			entry.visited = false
			prev := victimElem.Prev()
			if prev == nil {
				// 리스트의 시작에 도달하면 끝으로 이동
				victimElem = p.ll.Back()
			} else {
				victimElem = prev
			}
		} else {
			// 축출 대상 발견
			break
		}
	}

	// 다음 Evict를 위해 hand 위치를 업데이트
	prev := victimElem.Prev()
	if prev == nil {
		p.hand = p.ll.Back()
	} else {
		p.hand = prev
	}

	// 최종 희생자(victim) 제거
	victimEntry := p.removeElement(victimElem)
	return []string{victimEntry.key}
}

// removeElement는 내부 헬퍼 함수로, 주어진 list.Element를 제거하고 정리합니다.
func (p *SievePolicy) removeElement(e *list.Element) *sieveEntry {
	// 만약 제거될 요소가 hand가 가리키는 요소라면, hand를 이전 요소로 이동
	if e == p.hand {
		prev := e.Prev()
		if prev == nil {
			p.hand = p.ll.Back()
		} else {
			p.hand = prev
		}
	}

	p.ll.Remove(e)
	entry := e.Value.(*sieveEntry)
	delete(p.cache, entry.key)
	return entry
}
