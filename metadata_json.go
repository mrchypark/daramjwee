package daramjwee

import (
	"encoding/json"
	"time"
)

// MarshalJSON writes the new CacheTag field name while remaining compatible
// with older stored metadata that used ETag on disk.
func (m Metadata) MarshalJSON() ([]byte, error) {
	type payload struct {
		CacheTag   string      `json:"CacheTag,omitempty"`
		IsNegative bool        `json:"IsNegative"`
		CachedAt   interface{} `json:"CachedAt"`
	}
	return json.Marshal(payload{
		CacheTag:   m.CacheTag,
		IsNegative: m.IsNegative,
		CachedAt:   m.CachedAt,
	})
}

// UnmarshalJSON accepts both CacheTag and the legacy ETag field names.
func (m *Metadata) UnmarshalJSON(data []byte) error {
	type payload struct {
		CacheTag   string    `json:"CacheTag,omitempty"`
		LegacyETag string    `json:"ETag,omitempty"`
		IsNegative bool      `json:"IsNegative"`
		CachedAt   time.Time `json:"CachedAt"`
	}
	var p payload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	m.CacheTag = p.CacheTag
	if m.CacheTag == "" {
		m.CacheTag = p.LegacyETag
	}
	m.IsNegative = p.IsNegative
	m.CachedAt = p.CachedAt
	return nil
}
