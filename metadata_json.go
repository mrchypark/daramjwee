package daramjwee

import (
	"encoding/json"
	"time"
)

type metadataJSONPayload struct {
	CacheTag   string    `json:"CacheTag,omitempty"`
	LegacyETag string    `json:"ETag,omitempty"`
	IsNegative bool      `json:"IsNegative"`
	CachedAt   time.Time `json:"CachedAt"`
}

func metadataToJSONPayload(m Metadata) metadataJSONPayload {
	return metadataJSONPayload{
		CacheTag:   m.CacheTag,
		IsNegative: m.IsNegative,
		CachedAt:   m.CachedAt,
	}
}

func (p metadataJSONPayload) intoMetadata() Metadata {
	cacheTag := p.CacheTag
	if cacheTag == "" {
		cacheTag = p.LegacyETag
	}
	return Metadata{
		CacheTag:   cacheTag,
		IsNegative: p.IsNegative,
		CachedAt:   p.CachedAt,
	}
}

// MarshalJSON writes the new CacheTag field name while remaining compatible
// with older stored metadata that used ETag on disk.
func (m Metadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(metadataToJSONPayload(m))
}

// UnmarshalJSON accepts both CacheTag and the legacy ETag field names.
func (m *Metadata) UnmarshalJSON(data []byte) error {
	var p metadataJSONPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*m = p.intoMetadata()
	return nil
}
