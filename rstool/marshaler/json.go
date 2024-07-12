package marshaler

import "encoding/json"

var _ Marshaler = (*DefaultMarshaler)(nil)

type DefaultMarshaler struct{}

// Marshal implements Marshaler.
func (d *DefaultMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal implements Marshaler.
func (d *DefaultMarshaler) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
