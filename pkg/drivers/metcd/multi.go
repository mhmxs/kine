package metcd

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/client"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	return MultiETCD{}, nil
}

type MultiETCD struct {
}

func (d MultiETCD) Start(ctx context.Context) error {
	etcdClient, err := client.New(client.ETCDConfig{
		Endpoints: []string{"http://172.17.0.1:2379"},
	})
	if err != nil {
		return err
	}

	podClient, err := client.New(client.ETCDConfig{
		Endpoints: []string{"http://192.168.3.104:2379"},
	})
	if err != nil {
		return err
	}

	partitionTree[""] = map[string]*revisionClient{
		"": {
			Client: etcdClient,
			branch: "",
			leaf:   "",
		},
	}

	partitionTree["/registry/pods/"] = map[string]*revisionClient{
		"": {
			Client: etcdClient,
			branch: "/registry/pods/",
			leaf:   "",
		},
		"kube-system": {
			Client: podClient,
			branch: "/registry/pods/",
			leaf:   "kube-system",
		},
	}

	for b := range partitionTree {
		for l := range partitionTree[b] {
			currRev, err := partitionTree[b][l].Create(context.Background(), fmt.Sprintf("/kine/%s", time.Now().String()), nil)
			if err != nil {
				panic(err)
			}

			rc := partitionTree[b][l]
			rc.updateRevision(currRev)
		}
	}

	currentRevision()

	return err
}

func (d MultiETCD) DbSize(ctx context.Context) (int64, error) {
	return 0, nil
}

func (d MultiETCD) CurrentRevision(ctx context.Context) (int64, error) {
	return currentRevision(), nil
}

func (d MultiETCD) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *server.KeyValue, error) {
	backend := getClient(key)
	revision = revToOrig(revision, backend)

	currRev, value, err := backend.Client.Get(ctx, key,
		clientv3.WithRange(rangeEnd),
		clientv3.WithLimit(limit),
		clientv3.WithRev(revision),
	)
	backend.updateRevision(currRev)
	genRev := currentRevision()
	if err != nil || value.Created == 0 {
		if err == server.ErrKeyNotFound {
			err = nil
		}

		return genRev, nil, err
	}

	return genRev, &server.KeyValue{
		Key:            key,
		Value:          value.Data,
		CreateRevision: origToRev(backend, value.Created),
		ModRevision:    origToRev(backend, value.Modified),
		Lease:          value.Lease,
	}, nil
}

func (d MultiETCD) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	backend := getClient(key)

	opts := []clientv3.OpOption{}
	if lease != 0 {
		granted, err := backend.Client.Grant(ctx, lease)
		if err != nil {
			return currentRevision(), err
		}

		opts = append(opts, clientv3.WithLease(granted.ID))
	}

	currRev, err := backend.Client.Create(ctx, key, value, opts...)
	backend.updateRevision(currRev)

	return currentRevision(), err
}

func (d MultiETCD) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	backend := getClient(key)
	revision = revToOrig(revision, backend)

	opts := []clientv3.OpOption{}
	if lease != 0 {
		granted, err := backend.Client.Grant(ctx, lease)
		if err != nil {
			return currentRevision(), nil, false, err
		}

		opts = append(opts, clientv3.WithLease(granted.ID))
	}

	currRev, err := backend.Client.Update(ctx, key, revision, value, opts...)
	backend.updateRevision(currRev)
	genRev := currentRevision()
	if err != nil {
		return genRev, nil, false, err
	}

	_, curr, err := d.Get(ctx, key, "", 1, genRev)

	return genRev, curr, true, err
}

func (d MultiETCD) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	backend := getClient(key)
	revision = revToOrig(revision, backend)

	currRev, err := backend.Client.Delete(ctx, key, revision)
	backend.updateRevision(currRev)

	return currentRevision(), nil, err == nil, err
}

func (d MultiETCD) List(ctx context.Context, prefix, _ string, limit, revision int64) (int64, []*server.KeyValue, error) {
	backends := getClients(prefix)

	keyValues := []*server.KeyValue{}
	for i := range backends {
		backend := backends[i]
		revision := revToOrig(revision, backend)

		currRev, values, err := backend.Client.List(ctx, prefix, revision)
		if err != nil {
			return currentRevision(), nil, err
		}

		backend.updateRevision(currRev)

		for _, v := range values {
			keyValues = append(keyValues, &server.KeyValue{
				Key:            string(v.Key),
				Value:          v.Data,
				CreateRevision: origToRev(backend, v.Created),
				ModRevision:    origToRev(backend, v.Modified),
			})
		}
	}

	return currentRevision(), keyValues, nil
}

func (d MultiETCD) Count(ctx context.Context, prefix string, revision int64) (int64, int64, error) {
	currRev, values, err := d.List(ctx, prefix, "", math.MaxInt64, revision)

	// TODO performance
	return currRev, int64(len(values)), err
}

func (d MultiETCD) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	if revision == 0 {
		revision = currentRevision() + 1
	}

	backends := getClients(prefix)
	wg := sync.WaitGroup{}

	events := make(chan []*server.Event, 32)
	for i := range backends {
		wg.Add(1)

		backend := backends[i]
		revision := revToOrig(revision-1, backend) + 1

		watch := backend.Client.Watch(context.Background(), prefix,
			clientv3.WithRev(revision),
			clientv3.WithPrefix(),
			clientv3.WithPrevKV(),
		)

		go func() {
			defer wg.Done()

			for event := range watch {
				if event.Canceled || event.Err() != nil {
					return
				}

				backend.updateRevision(event.Header.Revision)

				for i := range event.Events {
					e := event.Events[i]

					event := server.Event{
						Create: e.IsCreate(),
						Delete: !e.IsCreate() && !e.IsModify(),
						KV: &server.KeyValue{
							Key:            string(e.Kv.Key),
							Value:          e.Kv.Value,
							CreateRevision: origToRev(backend, e.Kv.CreateRevision),
							ModRevision:    origToRev(backend, e.Kv.ModRevision),
							Lease:          e.Kv.Lease,
						},
						PrevKV: &server.KeyValue{},
					}

					if e.PrevKv != nil {
						event.PrevKV = &server.KeyValue{
							Key:            string(e.PrevKv.Key),
							Value:          e.PrevKv.Value,
							CreateRevision: origToRev(backend, e.PrevKv.CreateRevision),
							ModRevision:    origToRev(backend, e.PrevKv.ModRevision),
							Lease:          e.PrevKv.Lease,
						}

					}

					events <- []*server.Event{&event}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(events)
	}()

	return server.WatchResult{
		CurrentRevision: currentRevision(),
		Events:          events,
	}
}
