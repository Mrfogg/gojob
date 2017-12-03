package gojob

import (
	"sync"
	"time"

	"gojob/cron"

	"github.com/coreos/etcd/clientv3"
)

type jobCron struct {
	job    Job
	conStr string
}

type Gojob struct {
	httpPool *HTTPPool
	jobs     map[string]*jobCron
	mu       sync.RWMutex
	cr       *cron.Cron
	nodeName string
}

func NewGoJobByEtcd(client *clientv3.Client, key string, nodeName string) (*Gojob, error) {
	op := &HTTPPoolOptions{
		Replicas: 20,
	}
	etcd, err := newEtcd(client)
	if err != nil {
		return nil, err
	}
	hp := newHTTPPoolOpts(key, nodeName, op, etcd)
	return &Gojob{httpPool: hp, cr: cron.New(), jobs: make(map[string]*jobCron), nodeName: nodeName}, nil
}

func (g *Gojob) isMyJob(name string) bool {
	peer := g.httpPool.pickPeer(name)
	if peer == g.httpPool.Self {
		return true
	}
	return false
}
func (g *Gojob) AddJob(conStr string, j Job) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.jobs[j.GetName()] = &jobCron{job: j, conStr: conStr}
	if g.isMyJob(j.GetName()) {
		if conStr == "" {
			conStr = "0 */1 * * * ?"
		}
		g.cr.AddJob(conStr, j)
	}
}
func (g *Gojob) IsExist(name string) bool {
	g.mu.RLock()
	g.mu.RUnlock()
	if g.jobs[name] != nil {
		return true
	}
	return false
}

func (g *Gojob) DeletebyName(name string) {
	if g.jobs[name] != nil {
		g.cr.DeleteJobByName(name)
		delete(g.jobs, name)
	}
}

func (g *Gojob) stopbyName(name string) {
	g.cr.DeleteJobByName(name)
}

//定时检查自己的任务
func (g *Gojob) Update() {
	timer := time.NewTicker(time.Second * 10).C
	for {
		select {
		case <-timer:
			for name, _ := range g.jobs {
				if !g.isMyJob(name) {
					g.mu.Lock()
					g.stopbyName(name)
					delete(g.jobs, name)
					g.mu.Unlock()
				} else {
					if !g.cr.IsExist(name) {
						g.cr.AddJob(g.jobs["name"].conStr, g.jobs[name].job)
					}
				}
			}

		}
	}
}

func (g *Gojob) StartAll() {
	g.cr.Start()
	go g.Update()
}

func (g *Gojob) StopAll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for name, _ := range g.jobs {
		g.DeletebyName(name)

	}
}
