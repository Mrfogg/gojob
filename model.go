package gojob

type Job interface {
	//Init()
	Run()
	Stop()
	GetName() string
}
