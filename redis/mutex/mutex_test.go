package mutex

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEtcdClient(t *testing.T) {
	client, err := NewEtcdMutex(context.Background(), EtcdConfig{
		Timeout:   5 * time.Second,
		Endpoints: []string{"192.168.19.105:2379"},
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Nil(t, client.Lock("to_set"))
	assert.Nil(t, client.Unlock("to_set"))
}
