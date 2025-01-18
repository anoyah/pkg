package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/anoyah/pkgs/redis/marshaler"
	"github.com/anoyah/pkgs/redis/mutex"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// PrefixKey Redis key's key
type PrefixKey struct{}

var (
	ErrNotPtr = errors.New("reflectPtr need be a pointer")
)

// RsDataFn a function that returns data and expiration time
type RsDataFn func() (any, time.Duration, error)

// Rser interface for Rstool
type Rser interface {
	// Set
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	// Get
	Get(ctx context.Context, key string) (any, error)
	// Close
	Close() error
	// Cache
	Cache(ctx context.Context, key string, valueFn RsDataFn, reflect any, ops ...option) error
	// CacheWithRaw
	CacheRaw(ctx context.Context, key string, valueFn RsDataFn) ([]byte, error)
	// AddHook
	AddHook(hook redis.Hook)
	// InjectHotCache
	InjectHotCache(ctx context.Context, key string, valueFn RsDataFn) error
	// GetHotCache
	GetHotCache(ctx context.Context, key string, valuePtr any) error
	// ExpiredAt
	ExpiredAt(ctx context.Context, key string) (time.Duration, error)
	// GetInstance
	GetInstance() *redis.Client
}

// Cfg rstool config structure
type Cfg struct {
	Network    string
	Addr       string
	ClientName string
	Username   string
	Password   string
	DB         int
	TLSConfig  *tls.Config
	OnConnect  func(ctx context.Context, cn *redis.Conn) error
	PrefixKey  string
}

// extra options for rstool, like WithLogger, WithLocker and WithDebug
type option func(*Rstool)

// WithLogger set logger
func WithLogger(log Logger) option {
	return func(r *Rstool) {
		r.log = log
	}
}

// WithLocker set custom locker
func WithLocker(lock mutex.Locker) option {
	return func(r *Rstool) {
		r.l = lock
	}
}

// WithDebug for debug mode
func WithDebug() option {
	return func(r *Rstool) {
		r.debug = true
	}
}

func WithMarshal(msler marshaler.Marshaler) option {
	return func(r *Rstool) {
		r.msler = msler
	}
}

func WithProtoMarshal() option {
	return func(r *Rstool) {

	}
}

// Rstool rstool
type Rstool struct {
	// log logging
	log Logger
	// ctx context
	ctx context.Context
	// client redis client
	client *redis.Client
	// cfg rstool configuration
	cfg *Cfg
	// listenKey listening and then notify
	listenKey []string
	// single just run once
	single singleflight.Group
	// l for implemented Locker interface
	l mutex.Locker
	// mapRsDataFn Register data function
	mapRsDataFn map[string]RsDataFn
	// mu options for map
	mu sync.Mutex
	// chRefreshCache refresh cache channel
	chRefreshCache chan struct{}
	// debug develop mode for debug
	debug bool
	// msler implemented marshaler
	msler marshaler.Marshaler
}

// NewRstool create redis tools with config
func NewRstool(cfg *Cfg, opts ...option) (Rser, error) {
	rstool := &Rstool{
		cfg:            cfg,
		log:            defaultLogger(),
		ctx:            context.Background(),
		chRefreshCache: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(rstool)
	}
	// TODO needs hand set lock.
	// if rstool.l == nil {
	// 	rstool.l = &mutex.DefaultLock{}
	// }

	if cfg.PrefixKey != "" {
		rstool.ctx = context.WithValue(rstool.ctx, PrefixKey{}, cfg.PrefixKey)
	}

	rstool.client = redis.NewClient(&redis.Options{
		Network:    cfg.Network,
		Addr:       cfg.Addr,
		ClientName: cfg.ClientName,
		OnConnect:  cfg.OnConnect,
		Username:   cfg.Username,
		Password:   cfg.Password,
		DB:         cfg.DB,
		TLSConfig:  cfg.TLSConfig,
	})
	if err := rstool.client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	redis.SetLogger(rstool.log)
	rstool.AddHook(&defaultHook{Logger: rstool.log, prefix: cfg.PrefixKey})
	// register refresh service
	// go rstool.regRefreshEvent()

	// set default marshaler with json
	if rstool.msler == nil {
		rstool.msler = &marshaler.DefaultMarshaler{}
	}

	return rstool, nil
}

// GetInstance implements Rser.
func (r *Rstool) GetInstance() *redis.Client {
	return r.client
}

// CacheRaw implements Rser.
func (r *Rstool) CacheRaw(ctx context.Context, key string, valueFn RsDataFn) ([]byte, error) {
	panic("unimplemented")
}

// Cache implements Rser.
// Parma: opts like WithMarshaler
func (r *Rstool) Cache(ctx context.Context, key string, valueFn RsDataFn, reflectPtr any, opts ...option) error {
	if reflect.TypeOf(reflectPtr).Kind() != reflect.Pointer {
		return ErrNotPtr
	}

	for _, opt := range opts {
		opt(r)
	}

	var valueByte []byte
	result, err := r.get(ctx, key)
	if err != nil && err != redis.Nil {
		return err
	} else if err == nil {
		valueByte = []byte(result)
	} else if err == redis.Nil {
		// save data to redis and then return
		value, err := r.cache(ctx, key, valueFn)
		if err != nil {
			return err
		}
		switch value := value.(type) {
		case []byte:
			valueByte = value
		case string:
			valueByte = []byte(value)
		}
	}

	return r.msler.Unmarshal(valueByte, reflectPtr)
}

func (r *Rstool) cache(ctx context.Context, key string, valueFn RsDataFn) (any, error) {
	value, expiration, err := valueFn()
	if err != nil {
		return nil, err
	}
	content, err := r.msler.Marshal(value)
	if err != nil {
		return nil, err
	}

	if r.l == nil {
		if err := r.singleSet(ctx, key, content, expiration); err != nil {
			return nil, err
		}
	} else {
		if err := r.distributedSet(ctx, key, content, expiration); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// to set with single that using flightSingle of standard lib.
func (r *Rstool) singleSet(ctx context.Context, key string, value any, expiration time.Duration) error {
	_, err, _ := r.single.Do(key, func() (any, error) {
		if err := r.client.Set(ctx, joinPrefix(r.ctx, key), value, expiration).Err(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// to set with distributed service
func (r *Rstool) distributedSet(ctx context.Context, key string, value any, expiration time.Duration) error {
	if err := r.l.Lock(key); err != nil {
		return err
	}
	defer func() {
		_ = r.l.Unlock(key)
	}()
	_, err, _ := r.single.Do(key, func() (any, error) {
		if err := r.client.Set(ctx, joinPrefix(r.ctx, key), value, expiration).Err(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// Close implements Rser.
func (r *Rstool) Close() error {
	close(r.chRefreshCache)
	return r.client.Close()
}

// Get implements Rser.
func (r *Rstool) Get(ctx context.Context, key string) (any, error) {
	return r.get(ctx, key)
}

func (r *Rstool) get(ctx context.Context, key string) (string, error) {
	result := r.client.Get(ctx, joinPrefix(r.ctx, key))
	if err := result.Err(); err != nil {
		return "", err
	}

	return result.Val(), nil
}

// Set implements Rser.
func (r *Rstool) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.set(ctx, key, value, expiration)
}

func (r *Rstool) set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// InjectHotCache implements Rser.
func (r *Rstool) InjectHotCache(ctx context.Context, key string, valueFn RsDataFn) error {
	return r.injectCache(ctx, key, valueFn)
}

func (r *Rstool) injectCache(ctx context.Context, key string, valueFn RsDataFn) error {
	if r.mapRsDataFn == nil {
		r.mapRsDataFn = make(map[string]RsDataFn)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mapRsDataFn[key]; ok {
		return fmt.Errorf("already inject for key of cache: %s", key)
	} else {
		r.log.Log("register hot cache, key: %s", key)
		r.mapRsDataFn[key] = valueFn

		exist, err := r.exist(ctx, key)
		if err != nil {
			return err
		}
		if !exist {
			// call r.cache to cache Value that returns
			_, err := r.cache(ctx, key, valueFn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Rstool) exist(ctx context.Context, key string) (bool, error) {
	cmd := r.client.Exists(ctx, joinPrefix(r.ctx, key))
	if err := cmd.Err(); err != nil {
		return false, err
	}
	return cmd.Val() == 1, nil
}

func (r *Rstool) regRefreshEvent() {
	r.log.Log("register refresh event success")
	for {
		for range r.chRefreshCache {
			r.refreshCache()
		}
	}
}

func (r *Rstool) refreshCache() {
	for k, v := range r.mapRsDataFn {
		// TODO process error
		_, _ = r.cache(context.Background(), k, v)
	}
}

func (r *Rstool) listenRs() {
	// for k, v := range r.mapRsDataFn {

	// }
}

func (r *Rstool) getAllExpiredAt(_ context.Context) error {
	// for k, v := range r.mapRsDataFn {
	// 	expired, err := r.expiredAt(ctx, k)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	// expired.t
	// 	time.After()
	// 	time.Since()
	// }
	return nil
}

// ExpiredAt implements Rser.
func (r *Rstool) ExpiredAt(ctx context.Context, key string) (time.Duration, error) {
	return r.expiredAt(ctx, key)
}

// returns expired time and error
func (r *Rstool) expiredAt(ctx context.Context, key string) (time.Duration, error) {
	cmd := r.client.ExpireTime(ctx, joinPrefix(r.ctx, key))
	if err := cmd.Err(); err != nil {
		return 0, err
	}

	return cmd.Val(), nil
}

// GetHotCache implements Rser.
func (r *Rstool) GetHotCache(ctx context.Context, key string, valuePtr any) error {
	panic("unimplemented")
}

// AddHook add hooks
func (r *Rstool) AddHook(hook redis.Hook) {
	r.client.AddHook(hook)
}

var _ redis.Hook = (*defaultHook)(nil)

type defaultHook struct {
	Logger
	prefix string
}

// DialHook implements redis.Hook.
func (d *defaultHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		d.Log("[rstool] addr: %s", network, addr)
		return next(ctx, network, addr)
	}
}

// ProcessHook implements redis.Hook.
func (d *defaultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		d.Log("[rstool] cmd: %s", cmd.String())
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook implements redis.Hook.
func (d *defaultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		return next(ctx, cmds)
	}
}

// merge prefix and key if prefix is not empty
func joinPrefix(ctx context.Context, key string) string {
	prefix, _ := ctx.Value(PrefixKey{}).(string)

	if prefix == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", prefix, key)
}
