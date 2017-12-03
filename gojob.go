package gojob

import (
	"errors"
	"fmt"
	"sync"

	"gojob/cron"
	"truxing/commons/conf"
	"truxing/commons/libs/etcd"

	"time"

	"github.com/BurntSushi/toml"
)

type Gojob struct {
	httpPool *HTTPPool
	jobs     map[string]Job
	crons    map[string]string
	mu       sync.RWMutex
	cr       *cron.Cron
}

func NewGoJob(configPath string, key string) (*Gojob, error) {
	config := make(map[string]interface{})
	_, err := toml.DecodeFile(configPath, &config)
	if err != nil {
		return nil, err
	}
	host := config["EtcdHost"]
	if host == "" {
		return nil, errors.New("New gojob error:config file do not contain EtcdHost")
	}
	op := &HTTPPoolOptions{
		Replicas: 20,
	}
	et, err := etcd.NewEtcd(&conf.DbConfig{Host: fmt.Sprintf("%s", host)})
	if err != nil {
		return nil, errors.New("New gojob error:new etcd err " + err.Error())
	}
	hp := newHTTPPoolOpts(key, op, et)
	return &Gojob{httpPool: hp, cr: cron.New(), jobs: make(map[string]Job), crons: make(map[string]string)}, nil
}

func NewGoJobByEtcd(etcd *etcd.EtcdDb, key string) (*Gojob, error) {
	op := &HTTPPoolOptions{
		Replicas: 20,
	}

	hp := newHTTPPoolOpts(key, op, etcd)
	return &Gojob{httpPool: hp, cr: cron.New(), jobs: make(map[string]Job), crons: make(map[string]string)}, nil
}

func (g *Gojob) isMyJob(name string) bool {
	peer := g.httpPool.PickPeer(name)
	if peer == g.httpPool.Self {
		return true
	}
	return false
}
func (g *Gojob) AddJob(conStr string, j Job) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.jobs[j.GetName()] = j
	g.crons[j.GetName()] = conStr
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
		delete(g.crons, name)
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
						g.cr.AddJob(g.crons[name], g.jobs[name])
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
