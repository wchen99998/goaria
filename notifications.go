package goaria

import "time"

func (e *Engine) Subscribe(buffer int) (<-chan Notification, func()) {
	if buffer <= 0 {
		buffer = 32
	}
	ch := make(chan Notification, buffer)
	e.subMu.Lock()
	e.subscribers[ch] = struct{}{}
	e.subMu.Unlock()
	cancel := func() {
		e.subMu.Lock()
		if _, ok := e.subscribers[ch]; ok {
			delete(e.subscribers, ch)
			close(ch)
		}
		e.subMu.Unlock()
	}
	return ch, cancel
}

func (e *Engine) notify(method, gid string) {
	n := Notification{Method: method, GID: gid, Time: time.Now()}
	e.subMu.Lock()
	for ch := range e.subscribers {
		select {
		case ch <- n:
		default:
		}
	}
	e.subMu.Unlock()
}
