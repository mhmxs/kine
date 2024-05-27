package client

import (
	"context"
	"fmt"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
	LeaderElect bool
}

type Value struct {
	Key      []byte
	Data     []byte
	Created  int64
	Modified int64
	Lease    int64
}

type Client interface {
	List(ctx context.Context, key string, rev int64, opts ...clientv3.OpOption) (int64, []Value, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (int64, Value, error)
	Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error)
	Create(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error)
	Update(ctx context.Context, key string, revision int64, value []byte, opts ...clientv3.OpOption) (int64, error)
	Delete(ctx context.Context, key string, revision int64, opts ...clientv3.OpOption) (int64, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error)
	Close() error
}

type client struct {
	c *clientv3.Client
}

func New(config ETCDConfig) (Client, error) {
	tlsConfig, err := config.TLSConfig.ClientConfig()
	if err != nil {
		return nil, err
	}

	c, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		return nil, err
	}

	return &client{
		c: c,
	}, nil
}

func (c *client) List(ctx context.Context, key string, rev int64, opts ...clientv3.OpOption) (int64, []Value, error) {
	if opts == nil {
		opts = []clientv3.OpOption{
			clientv3.WithPrefix(),
		}
	}
	opts = append(opts, clientv3.WithRev(int64(rev)))

	resp, err := c.c.Get(ctx, key, opts...)
	if err != nil {
		return 0, nil, err
	}

	var vals []Value
	for _, kv := range resp.Kvs {
		vals = append(vals, Value{
			Key:      kv.Key,
			Data:     kv.Value,
			Created:  kv.CreateRevision,
			Modified: kv.ModRevision,
			Lease:    kv.Lease,
		})
	}

	return resp.Header.Revision, vals, nil
}

func (c *client) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (int64, Value, error) {
	resp, err := c.c.Get(ctx, key, opts...)
	if err != nil {
		return resp.Header.Revision, Value{}, err
	}
	if len(resp.Kvs) == 1 {
		return resp.Header.Revision, Value{
			Key:      resp.Kvs[0].Key,
			Data:     resp.Kvs[0].Value,
			Created:  resp.Kvs[0].CreateRevision,
			Modified: resp.Kvs[0].ModRevision,
			Lease:    resp.Kvs[0].Lease,
		}, nil
	}
	return resp.Header.Revision, Value{}, server.ErrKeyNotFound
}

func (c *client) Put(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	rev, val, err := c.Get(ctx, key, opts...)
	if err != nil {
		return rev, err
	}
	if val.Modified == 0 {
		return c.Create(ctx, key, value, opts...)
	}
	return c.Update(ctx, key, val.Modified, value, opts...)
}

func (c *client) Create(ctx context.Context, key string, value []byte, opts ...clientv3.OpOption) (int64, error) {
	resp, err := c.c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value), opts...)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return resp.Header.Revision, err
	}
	if !resp.Succeeded {
		return resp.Header.Revision, server.ErrKeyExists
	}
	return resp.Header.Revision, nil
}

func (c *client) Update(ctx context.Context, key string, revision int64, value []byte, opts ...clientv3.OpOption) (int64, error) {
	resp, err := c.c.Txn(ctx).
		If(). // TODO sometimes it compares with generated revision ??? clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpPut(key, string(value), opts...)).
		Commit()
	if err != nil {
		return resp.Header.Revision, err
	}
	if !resp.Succeeded {
		return resp.Header.Revision, server.ErrKeyExists
	}
	return resp.Header.Revision, nil
}

func (c *client) Delete(ctx context.Context, key string, revision int64, opts ...clientv3.OpOption) (int64, error) {
	resp, err := c.c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpDelete(key, opts...)).
		Commit()
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return resp.Header.Revision, fmt.Errorf("revision %d doesnt match", revision)
	}
	return resp.Header.Revision, nil
}

func (c *client) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return c.c.Watch(ctx, key, opts...)
}

func (c *client) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return c.c.Lease.Grant(ctx, ttl)
}

func (c *client) Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error) {
	return c.c.Lease.Leases(ctx)
}

func (c *client) Close() error {
	return c.c.Close()
}
