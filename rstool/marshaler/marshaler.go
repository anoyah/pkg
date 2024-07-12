package marshaler

// Marshaler 序列化
type Marshaler interface {
	Unmarshal(data []byte, v any) error
	Marshal(v any) ([]byte, error)
}
