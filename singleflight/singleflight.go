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

import "sync"

// call is an in-flight or completed Do call
type call struct {
	// chans holds one buffered channel per waiting caller. Each
	// channel receives exactly one Result when the call completes.
	chans []chan<- Result
}

// Result holds the results of a call, as delivered on the channel
// returned by DoChan.
type Result struct {
	Val interface{}
	Err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	res := <-g.DoChan(key, fn)
	return res.Val, res.Err
}

// DoChan is like Do but returns a channel that will receive the
// results when they are ready. The channel is buffered and receives
// exactly one result, so a caller is free to stop waiting (for
// example when its own context is cancelled) without blocking or
// leaking the shared call, and without affecting other callers
// waiting on the same key.
func (g *Group) DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	ch := make(chan Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.chans = append(c.chans, ch)
		g.mu.Unlock()
		return ch
	}
	c := new(call)
	c.chans = []chan<- Result{ch}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		val, err := fn()

		g.mu.Lock()
		delete(g.m, key)
		chans := c.chans
		g.mu.Unlock()

		res := Result{Val: val, Err: err}
		for _, ch := range chans {
			ch <- res
		}
	}()

	return ch
}
