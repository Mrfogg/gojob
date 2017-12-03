package gojob

import (
	"gojob/consistenthash"
	"sync"
	"time"

	"log"

	"github.com/coreos/etcd/mvcc/mvccpb"
)

const defaultReplicas = 50

type HTTPPool struct {
	Self string

	// opts specifies the options.
	opts  HTTPPoolOptions
	mu    sync.Mutex
	peers *consistenthash.Map

	etcdDb *etcdDb

	key      string
	nodeName string
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash
}

var httpPoolMade bool

func newHTTPPoolOpts(prefix string, nodeName string, o *HTTPPoolOptions, et *etcdDb) *HTTPPool {
	if httpPoolMade {
		panic("fame_collect: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		etcdDb:   et,
		key:      prefix,
		nodeName: nodeName,
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.initPeers()
	go p.set()
	go p.heartBeat()
	return p
}
func (p *HTTPPool) initPeers() {
	p.Self = p.key + p.nodeName
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(p.Self)
	mp, _ := p.etcdDb.GetPrefix(p.key)
	for k, _ := range mp {
		p.peers.Add(k)
	}
}

func (p *HTTPPool) set() {
	for c := range p.etcdDb.WatchPrefix(p.key) {
		p.mu.Lock()
		if c.Events[0].Type == mvccpb.PUT {
			p.peers.Add(string(c.Events[0].Kv.Key))
			log.Printf("put node %s", c.Events[0].Kv.Key)
		}
		if c.Events[0].Type == mvccpb.DELETE {
			p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
			mp, _ := p.etcdDb.GetPrefix(p.key)
			for k, _ := range mp {
				p.peers.Add(k)
			}
			log.Printf("node %s dead", c.Events[0].Kv.Key)
		}
		p.mu.Unlock()
	}
}

func (p *HTTPPool) pickPeer(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return ""
	}
	peer := p.peers.Get(key)
	return peer
}

func (p *HTTPPool) heartBeat() {
	tick := time.Tick(time.Second * 15)

	for {
		select {
		case <-tick:
			p.etcdDb.PutTTL(p.Self, "", time.Second*25)
		}
	}

}
