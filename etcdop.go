package gojob

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type etcdDb struct {
	client *clientv3.Client
}

func newEtcd(client *clientv3.Client) (*etcdDb, error) {
	et := new(etcdDb)
	et.client = client
	return et, nil
}

func (et *etcdDb) PutTTL(key string, value string, ttl time.Duration) error {
	g, err := et.client.Grant(context.TODO(), int64(ttl/time.Second))
	if err != nil {
		return err
	}
	_, err = et.client.Put(context.TODO(), key, value, clientv3.WithLease(g.ID))
	if err != nil {
		return err
	}
	return nil
}

func (et *etcdDb) Put(key string, value string) error {
	_, err := et.client.Put(context.TODO(), key, value)
	if err != nil {
		return err
	}
	return nil
}

func (et *etcdDb) Delete(key string) error {
	_, err := et.client.Delete(context.TODO(), key)
	if err != nil {
		return err
	}
	return nil
}

func (et *etcdDb) Get(key string) (string, error) {
	resp, err := et.client.Get(context.TODO(), key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	val := resp.Kvs[0].Value
	return string(val), nil

}

func (et *etcdDb) WatchPrefix(prefix string) clientv3.WatchChan {
	ch := et.client.Watch(context.TODO(), prefix, clientv3.WithPrefix())
	return ch
}

func (et *etcdDb) GetPrefix(key string) (map[string]string, error) {
	resp, err := et.client.Get(context.TODO(), key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	mp := make(map[string]string)
	for _, kv := range resp.Kvs {
		mp[string(kv.Key)] = string(kv.Value)
	}
	return mp, nil

}
