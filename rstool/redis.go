package rstool

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const (
	PREFIX_KEY = "prefix"
)

// Rser interface for Rstool
type Rser interface {
	// Cache
	Cache(ctx context.Context, key string, valueFn func() (any, time.Duration, error), reflect any) error
	// InjectHotCache
	InjectHotCache(ctx context.Context, key string, valueFn func() (any, time.Duration, error), reflect any) error
	// GetHotCache
	GetHotCache(ctx context.Context, key string, valuePtr any) error
	// Set
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	// Get
	Get(ctx context.Context, key string) (any, error)
	// AddHook
	AddHook(hook redis.Hook)
	// Close
	Close() error
}

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

type option func(*Rstool)

// WithLogger set logger
func WithLogger(log Logger) option {
	return func(r *Rstool) {
		r.Logger = log
	}
}

// Rstool rstool
type Rstool struct {
	// Logger logging
	Logger
	// ctx context
	ctx context.Context
	// client redis client
	client *redis.Client
	// cfg rstool configuration
	cfg *Cfg
	// listenKey listening and then notify
	listenKey []string
	single    singleflight.Group
}

// GetHotCache implements Rser.
func (r *Rstool) GetHotCache(ctx context.Context, key string, valuePtr any) error {
	panic("unimplemented")
}

// NewRstool create redis tools with config
func NewRstool(cfg *Cfg, opts ...option) (Rser, error) {
	rstool := &Rstool{
		cfg:    cfg,
		Logger: defaultLogger(),
		ctx:    context.Background(),
	}
	for _, opt := range opts {
		opt(rstool)
	}

	if cfg.PrefixKey != "" {
		rstool.ctx = context.WithValue(rstool.ctx, PREFIX_KEY, cfg.PrefixKey)
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

	redis.SetLogger(rstool.Logger)
	rstool.AddHook(&defaultHook{Logger: rstool.Logger, prefix: cfg.PrefixKey})

	return rstool, nil
}

// Cache implements Rser.
func (r *Rstool) Cache(ctx context.Context, key string, valueFn func() (any, time.Duration, error), reflectPtr any) error {
	if reflect.TypeOf(reflectPtr).Kind() != reflect.Pointer {
		return errors.New("reflectPtr need be a pointer")
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
		switch value.(type) {
		case []byte:
			valueByte = value.([]byte)
		case string:
			valueByte = []byte(value.(string))
		}
	}

	return json.Unmarshal(valueByte, reflectPtr)
}

// InjectHotCache implements Rser.
func (r *Rstool) InjectHotCache(ctx context.Context, key string, valueFn func() (any, time.Duration, error), reflect any) error {

	panic("unimplemented")
}

func (r *Rstool) addListen(ctx context.Context, key string) {
	r.listenKey = append(r.listenKey, key)

}

func (r *Rstool) cache(ctx context.Context, key string, valueFn func() (any, time.Duration, error)) (any, error) {
	value, expired, err := valueFn()
	if err != nil {
		return nil, err
	}

	if err := r.singleSet(ctx, key, value, expired); err != nil {
		return nil, err
	}
	return value, nil
}

func (r *Rstool) singleSet(ctx context.Context, key string, value any, expiration time.Duration) error {
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
		if d.prefix != "" {
			if len(cmd.Args()) > 1 {
				// cmd.SetFirstKeyPos()
			}
		}
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
	prefix, _ := ctx.Value(PREFIX_KEY).(string)

	if prefix == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", prefix, key)
}
