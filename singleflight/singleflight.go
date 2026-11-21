/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"sync"
	"time"
)

// DefaultErrSuppressWindow is the default amount of time the result of
// a failed Do call is kept for duplicate suppression.  See
// Group.ErrSuppressWindow.
const DefaultErrSuppressWindow = 100 * time.Millisecond

// call is an in-flight or completed Do call
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	// ErrSuppressWindow is how long the error result of a failed
	// call remains available for duplicate suppression after the
	// call completes.  While a failed call is retained, additional
	// Do calls for the same key (e.g. retries) receive the same
	// error instead of re-invoking fn, so a burst of failures
	// against a slow or struggling backend collapses into a single
	// execution.  Successful calls are never retained: their
	// duplicates are only suppressed while in-flight, as before.
	//
	// If zero, DefaultErrSuppressWindow is used.  If negative,
	// failed calls are forgotten immediately (the historical
	// behavior).
	ErrSuppressWindow time.Duration

	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

func (g *Group) errSuppressWindow() time.Duration {
	if g.ErrSuppressWindow == 0 {
		return DefaultErrSuppressWindow
	}
	return g.ErrSuppressWindow
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	if c.err != nil {
		if window := g.errSuppressWindow(); window > 0 {
			// Keep the failed call in the map for a short
			// window so that concurrent retries for the same
			// key are suppressed onto this error instead of
			// each re-invoking fn against the backend.
			time.AfterFunc(window, func() {
				g.mu.Lock()
				if g.m[key] == c {
					delete(g.m, key)
				}
				g.mu.Unlock()
			})
			return c.val, c.err
		}
	}

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
