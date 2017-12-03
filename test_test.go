package gojob

import (
	"testing"

	"fmt"

	"github.com/coreos/etcd/clientv3"
)

var client *clientv3.Client

//初始化etcd client对象
func init() {
	var err error
	client, err = clientv3.New(clientv3.Config{Endpoints: []string{
		"localhost:2379",
	}})
	if err != nil {
		panic("new client of etcd fail")
	}
}

//实现job接口
type testJob struct {
}

func (t *testJob) GetName() string {
	return "testjob"
}
func (t *testJob) Run() {
	fmt.Println("test job runing")
}
func (t *testJob) Stop() {
	fmt.Println("test job stop")
}
func TestGojob(t *testing.T) {
	gj, err := NewGoJobByEtcd(client, "test", "node1")
	if err != nil {
		t.Error("new gojob fail")
	}
	gj.AddJob("*/5 * * * * ?", new(testJob)) //cron表达式控制执行时间
	gj.StartAll()
	c := make(chan int) //这里的作用更是让主协程序挂起等待
	<-c
}
