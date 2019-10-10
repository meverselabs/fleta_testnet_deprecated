package debug

import (
	"log"
	"sync"
	"time"
)

var gProfiler = &Profiler{
	PointTimeMap:  map[string]int64{},
	PointCountMap: map[string]int{},
}

func Start(name string) *Timer {
	return gProfiler.Start(name)
}

func Result() {
	gProfiler.Result()
}

type Profiler struct {
	sync.Mutex
	PointTimeMap  map[string]int64
	PointCountMap map[string]int
}

func (p *Profiler) Start(name string) *Timer {
	return &Timer{
		p:     p,
		Name:  name,
		Begin: time.Now().UnixNano(),
	}
}

func (p *Profiler) Result() {
	p.Lock()
	defer p.Unlock()

	for name, t := range p.PointTimeMap {
		cnt := int64(p.PointCountMap[name])
		if cnt > 0 {
			log.Println(name, time.Duration(t), cnt, time.Duration(t/cnt))
		} else {
			log.Println(name, time.Duration(t), cnt)
		}
	}
	p.PointTimeMap = map[string]int64{}
	p.PointCountMap = map[string]int{}
}

type Timer struct {
	p     *Profiler
	Name  string
	Begin int64
}

func (t *Timer) Stop() {
	t.p.Lock()
	defer t.p.Unlock()

	t.p.PointTimeMap[t.Name] += time.Now().UnixNano() - t.Begin
	t.p.PointCountMap[t.Name]++
}
