package goaria

import "time"

const maxNotificationBuffer = 4096

func (e *Engine) Subscribe(buffer int) (<-chan Notification, func()) {
	if buffer <= 0 {
		buffer = 32
	} else if buffer > maxNotificationBuffer {
		buffer = maxNotificationBuffer
	}
	ch := make(chan Notification, buffer)
	e.subMu.Lock()
	if e.ctx.Err() != nil {
		close(ch)
	} else {
		e.subscribers[ch] = struct{}{}
	}
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

func (e *Engine) closeSubscribers() {
	e.subMu.Lock()
	for ch := range e.subscribers {
		delete(e.subscribers, ch)
		close(ch)
	}
	e.subMu.Unlock()
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
