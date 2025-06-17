// Filename: policy/sieve_policy_test.go
package policy

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// getEntry는 테스트 목적으로 list.Element에서 sieveEntry를 안전하게 추출합니다.
func getEntry(p *SievePolicy, key string) *sieveEntry {
	if elem, ok := p.cache[key]; ok {
		return elem.Value.(*sieveEntry)
	}
	return nil
}

// --- Happy Path Tests ---

// TestSievePolicy_BasicAddAndTouch는 가장 기본적인 Add와 Touch 동작을 검증합니다.
func TestSievePolicy_BasicAddAndTouch(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)

	// 1. 새 아이템 추가
	p.Add("A", 10)
	assert.Equal(t, 1, p.ll.Len(), "리스트에 아이템이 하나 추가되어야 합니다.")
	assert.Equal(t, 1, len(p.cache), "캐시 맵에 아이템이 하나 추가되어야 합니다.")

	entryA := getEntry(p, "A")
	assert.NotNil(t, entryA, "캐시에서 'A'를 찾을 수 있어야 합니다.")
	assert.False(t, entryA.visited, "처음 추가된 아이템의 visited 플래그는 false여야 합니다.")

	// 2. 아이템에 접근 (Touch)
	p.Touch("A")
	assert.True(t, entryA.visited, "Touch 이후 visited 플래그는 true여야 합니다.")

	// 3. 존재하지 않는 아이템에 Touch해도 패닉이 발생하지 않아야 합니다.
	assert.NotPanics(t, func() { p.Touch("B") })
}

// TestSievePolicy_AddExisting는 이미 존재하는 아이템을 다시 Add할 때의 동작을 검증합니다.
func TestSievePolicy_AddExisting(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 10)
	p.Add("B", 20) // 리스트: [B, A]

	// 1. 기존 아이템 "A"를 다른 사이즈로 다시 추가
	p.Add("A", 100)
	assert.Equal(t, 2, p.ll.Len(), "전체 아이템 개수는 동일해야 합니다.")

	// 2. "A"가 리스트의 맨 앞으로 이동했는지 확인
	assert.Equal(t, "A", p.ll.Front().Value.(*sieveEntry).key, "'A'가 리스트의 맨 앞으로 이동해야 합니다.")

	// 3. "A"의 정보가 올바르게 업데이트되었는지 확인
	entryA := getEntry(p, "A")
	assert.Equal(t, int64(100), entryA.size, "사이즈가 업데이트되어야 합니다.")
	assert.True(t, entryA.visited, "기존 아이템을 다시 Add하면 visited는 true가 되어야 합니다.")
}

// TestSievePolicy_Remove는 아이템 제거가 정상적으로 동작하는지 검증합니다.
func TestSievePolicy_Remove(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 10)
	p.Add("B", 20)
	p.Add("C", 30) // 리스트: [C, B, A]

	// 1. 중간 아이템 "B" 제거
	p.Remove("B")
	assert.Equal(t, 2, p.ll.Len(), "아이템 제거 후 리스트 길이가 줄어야 합니다.")
	assert.Nil(t, getEntry(p, "B"), "'B'는 캐시에서 제거되어야 합니다.")
	assert.NotNil(t, getEntry(p, "A"), "'A'는 여전히 존재해야 합니다.")
	assert.NotNil(t, getEntry(p, "C"), "'C'는 여전히 존재해야 합니다.")

	// 2. 존재하지 않는 아이템 제거 시 패닉이 없어야 함
	assert.NotPanics(t, func() { p.Remove("D") })
	assert.Equal(t, 2, p.ll.Len(), "존재하지 않는 아이템 제거는 아무런 영향을 주지 않아야 합니다.")
}

// --- Eviction Logic Tests ---

// TestSievePolicy_Evict_MainScenario는 SIEVE 알고리즘의 핵심 시나리오를 검증합니다.
// 자주 접근하는 객체(popular)는 살아남고, 거의 접근하지 않는 객체(one-hit wonder)는 축출되어야 합니다.
func TestSievePolicy_Evict_MainScenario(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	oneHitWonders := []string{"1", "2", "3", "4", "5"}
	popularObjects := []string{"6", "7", "8", "9", "10"}

	// 1. 모든 객체 추가
	for _, v := range oneHitWonders {
		p.Add(v, 1)
	}
	for _, v := range popularObjects {
		p.Add(v, 1)
	}
	// 리스트 상태 (맨 앞): [10, 9, ... 2, 1]

	// 2. popular 객체만 접근 (Touch)
	for _, v := range popularObjects {
		p.Touch(v)
	}
	// popular 객체들의 visited 플래그가 true가 됨

	// 3. 5개의 새로운 객체를 추가하여 5번의 축출을 유발한다고 가정하고 Evict 실행
	//    (실제 Store에서는 캐시가 꽉 찼을 때 Evict를 호출)

	// 4. 축출 로직 실행 및 검증 (one-hit wonders가 순서대로 축출되어야 함)
	for i := 0; i < len(oneHitWonders); i++ {
		evicted := p.Evict()
		expectedVictim := oneHitWonders[i]
		assert.Equal(t, []string{expectedVictim}, evicted, fmt.Sprintf("%d번째 축출 대상은 '%s'여야 합니다.", i+1, expectedVictim))
	}

	// 5. popular 객체들은 여전히 캐시에 남아있어야 함
	for _, v := range popularObjects {
		assert.NotNil(t, getEntry(p, v), fmt.Sprintf("popular 객체 '%s'는 축출되지 않아야 합니다.", v))
	}
}

// TestSievePolicy_Evict_FullRotation은 모든 아이템이 visited일 때,
// hand가 전체 리스트를 스캔하고 처음 스캔한 아이템을 축출하는지 검증합니다.
func TestSievePolicy_Evict_FullRotation(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 1)
	p.Add("B", 1)
	p.Add("C", 1) // 리스트: [C, B, A]

	// 모든 아이템에 접근
	p.Touch("A")
	p.Touch("B")
	p.Touch("C")

	// Evict 호출
	// 1. hand는 A를 가리킴. A.visited = true -> false. hand는 B로 이동.
	// 2. hand는 B를 가리킴. B.visited = true -> false. hand는 C로 이동.
	// 3. hand는 C를 가리킴. C.visited = true -> false. hand는 리스트의 끝으로(A로) 되돌아감.
	// 4. hand는 A를 다시 가리킴. A.visited는 이제 false이므로 A가 최종 축출 대상이 됨.
	evicted := p.Evict()
	assert.Equal(t, []string{"A"}, evicted, "모두 visited일 경우, 가장 오래된 아이템('A')이 축출되어야 합니다.")
}

// --- Edge Case & Failure Case Tests ---

// TestSievePolicy_EvictEmpty는 빈 정책에서 Evict 호출 시 nil을 반환하는지 검증합니다.
func TestSievePolicy_EvictEmpty(t *testing.T) {
	p := NewSievePolicy()
	assert.Nil(t, p.Evict(), "빈 캐시에서 Evict()는 nil을 반환해야 합니다.")
}

// TestSievePolicy_EvictSingleItem는 아이템이 하나일 때 Evict가 잘 동작하는지 검증합니다.
func TestSievePolicy_EvictSingleItem(t *testing.T) {
	p := NewSievePolicy()
	p.Add("A", 10)

	evicted := p.Evict()
	assert.Equal(t, []string{"A"}, evicted, "아이템이 하나일 때, 해당 아이템이 축출되어야 합니다.")
	assert.Equal(t, 0, p.(*SievePolicy).ll.Len(), "축출 후 캐시는 비어있어야 합니다.")
}

// TestSievePolicy_RemoveHandledElement는 hand가 가리키는 아이템이 Remove될 때
// hand 포인터가 안전하게 조정되는지 검증합니다.
func TestSievePolicy_RemoveHandledElement(t *testing.T) {
	p := NewSievePolicy().(*SievePolicy)
	p.Add("A", 1)
	p.Add("B", 1)
	p.Add("C", 1) // 리스트: [C, B, A]

	// 1. Evict를 한 번 호출하여 hand를 설정 (A가 축출되고 hand는 B를 가리킴)
	p.Evict()
	assert.Equal(t, "B", p.hand.Value.(*sieveEntry).key, "첫 Evict 후 hand는 'B'를 가리켜야 합니다.")

	// 2. hand가 가리키는 'B'를 제거
	p.Remove("B")

	// 3. hand가 'B'의 이전 요소인 'C'로 이동했는지 확인
	assert.Equal(t, "C", p.hand.Value.(*sieveEntry).key, "hand가 가리키던 요소가 삭제되면, hand는 그 이전 요소를 가리켜야 합니다.")
}

// TestSievePolicy_Churn은 잦은 추가/삭제 상황에서 내부 상태가 깨지지 않는지 검증합니다.
func TestSievePolicy_Churn(t *testing.T) {
	p := NewSievePolicy()

	// 1. 캐시 채우기
	for i := 0; i < 10; i++ {
		p.Add(strconv.Itoa(i), 1)
	}

	// 2. 잦은 추가 및 축출 반복
	for i := 10; i < 1000; i++ {
		// Evict를 호출하여 공간을 만든다고 가정
		evicted := p.Evict()
		assert.NotNil(t, evicted, "Churn 테스트 중에는 항상 축출 대상이 있어야 합니다.")

		// 새 아이템 추가
		p.Add(strconv.Itoa(i), 1)

		// 내부 상태 검증 (패닉이 발생하지 않고, 리스트 길이가 일정하게 유지되는지)
		assert.Equal(t, 10, p.(*SievePolicy).ll.Len(), "Churn 테스트 중 리스트 길이는 일정해야 합니다.")
	}
}
