package gokv

import (
	"runtime/debug"
	"sync"
	"time"
)

const (
	defCheckInterval   = 0
	defExpDuration     = 0
	defFreeMemInterval = 0
)

type Store struct {
	mtx             sync.RWMutex
	items           map[string]Item
	expDuration     time.Duration
	checkInterval   time.Duration
	freeMemInterval time.Duration
	stopChecking    chan bool
	closed          bool
	wg              sync.WaitGroup
}

type Item struct {
	Value   interface{}
	ExpTime int64
	Created time.Time
}

func New() *Store {
	Items := make(map[string]Item)
	s := Store{
		items:           Items,
		expDuration:     defExpDuration,
		checkInterval:   defCheckInterval,
		freeMemInterval: defFreeMemInterval,
	}
	if s.checkInterval > 0 {
		s.stopChecking = make(chan bool, 1)
		s.startCheckingExpired()
	}
	return &s
}

func NewCustom(DefaultExpDuration, CheckInterval time.Duration) *Store {
	Items := make(map[string]Item)
	s := Store{
		items:           Items,
		expDuration:     DefaultExpDuration,
		checkInterval:   CheckInterval,
		freeMemInterval: defFreeMemInterval,
	}
	if s.checkInterval > 0 {
		s.stopChecking = make(chan bool, 1)
		s.startCheckingExpired()
	}
	return &s
}

// SetVal sets value
func (s *Store) SetVal(Key string, Value interface{}) {
	s.mtx.Lock()
	if s.closed {
		s.mtx.Unlock()
		return
	}
	s.items[Key] = Item{
		Value:   Value,
		ExpTime: time.Now().Add(s.expDuration).UnixNano(),
		Created: time.Now(),
	}
	s.mtx.Unlock()
}

// SetValWithCustomExp sets value with customized expiration
func (s *Store) SetValWithCustomExp(Key string, Value interface{}, Expiration time.Duration) {
	var exp int64
	if Expiration > 0 {
		exp = time.Now().Add(Expiration).UnixNano()
	} else {
		exp = time.Now().Add(s.expDuration).UnixNano()
	}
	s.mtx.Lock()
	if s.closed {
		s.mtx.Unlock()
		return
	}
	s.items[Key] = Item{
		Value:   Value,
		ExpTime: exp,
		Created: time.Now(),
	}
	s.mtx.Unlock()
}

// GetVal returns value
func (s *Store) GetVal(Key string) (interface{}, bool) {
	s.mtx.RLock()
	if s.closed {
		s.mtx.RUnlock()
		return nil, false
	}
	item, found := s.items[Key]
	s.mtx.RUnlock()
	if !found {
		return nil, false
	}
	if item.ExpTime > 0 {
		if time.Now().UnixNano() > item.ExpTime {
			return nil, false
		}
	}
	return item.Value, true
}

// Delete deletes value
func (s *Store) Delete(Key string) {
	s.mtx.RLock()
	if s.closed {
		s.mtx.RUnlock()
		return
	}
	s.mtx.RUnlock()
	s.mtx.Lock()
	delete(s.items, Key)
	s.mtx.Unlock()
}

// Close gracefully closes the Store
func (s *Store) Close() {
	s.mtx.RLock()
	if s.closed {
		s.mtx.RUnlock()
		return
	}
	s.mtx.RUnlock()
	if s.checkInterval > 0 {
		s.stopChecking <- true
		s.wg.Wait()
	}
	s.mtx.Lock()
	if s.items != nil {
		for k := range s.items {
			delete(s.items, k)
		}
	}
	s.mtx.Unlock()
	if s.freeMemInterval > 0 {
		debug.FreeOSMemory()
	}
	s.closed = true
}

// SetFreeMemInterval sets the min interval for execution of debug.FreeOSMemory
func (s *Store) SetFreeMemInterval(NewFreeMemInterval time.Duration) {
	s.mtx.Lock()
	s.freeMemInterval = NewFreeMemInterval
	s.mtx.Unlock()
}

func (s *Store) startCheckingExpired() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var timeNow, timeFreeMemTS int64
		timeFreeMemTS = time.Now().UnixNano()
		for {
			select {
			case <-time.After(s.checkInterval):
				timeNow = time.Now().UnixNano()
				s.mtx.Lock()
				for k := range s.items {
					if timeNow > s.items[k].ExpTime && s.items[k].ExpTime > 0 {
						delete(s.items, k)
					}
				}
				s.mtx.Unlock()
				s.mtx.RLock()
				if s.freeMemInterval > 0 &&
					timeFreeMemTS < timeNow-s.freeMemInterval.Nanoseconds() {
					timeFreeMemTS = timeNow
					debug.FreeOSMemory()
				}
				s.mtx.RUnlock()
			case <-s.stopChecking:
				close(s.stopChecking)
				return
			}
		}
	}()
}
