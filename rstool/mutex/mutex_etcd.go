package mutex

import (
	"context"
	"time"

	v3 "go.etcd.io/etcd/client/v3"
)

type EtcdMutex struct {
	client  *v3.Client
	ctx     context.Context
	timeout time.Duration //do handle expired time duration
	expired *v3.LeaseGrantResponse
}

type EtcdConfig struct {
	Timeout   time.Duration // do handle expired time duration
	Endpoints []string
}

func NewEtcdMutex(ctx context.Context, cfg EtcdConfig) (Locker, error) {
	// set timeout as 5s when timeout is empty(0)
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second * 5
	}
	client, err := v3.New(v3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: time.Second * 3,
	})
	if err != nil {
		return nil, err
	}

	// set ttl 10 seconds
	leash, err := client.Grant(ctx, 10)
	if err != nil {
		return nil, err
	}

	return &EtcdMutex{
		client:  client,
		ctx:     ctx,
		timeout: cfg.Timeout,
		expired: leash,
	}, nil
}

// Lock implements Locker.
func (e *EtcdMutex) Lock(key string) error {
	ctx, cancel := context.WithTimeout(e.ctx, e.timeout)
	defer cancel()

	txn := e.client.Txn(ctx).
		If(v3.Compare(v3.CreateRevision(key), "=", 0)).
		Then(v3.OpPut(key, "", v3.WithLease(e.expired.ID))).Else()
	resp, err := txn.Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return ErrNotAcquireLock
	}
	return nil
}

// Unlock implements Locker.
func (e *EtcdMutex) Unlock(key string) error {
	ctx, cancel := context.WithTimeout(e.ctx, e.timeout)
	defer cancel()

	_, err := e.client.Delete(ctx, key)
	if err != nil {
		return err
	}
	return nil
}
