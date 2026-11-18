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

package singleflight

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	var g Group
	v, err := g.Do("key", func() (interface{}, error) {
		return "bar", nil
	})
	if got, want := fmt.Sprintf("%v (%T)", v, v), "bar (string)"; got != want {
		t.Errorf("Do = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

func TestDoErr(t *testing.T) {
	var g Group
	someErr := errors.New("some error")
	v, err := g.Do("key", func() (interface{}, error) {
		return nil, someErr
	})
	if err != someErr {
		t.Errorf("Do error = %v; want someErr", err)
	}
	if v != nil {
		t.Errorf("unexpected non-nil value %#v", v)
	}
}

func TestDoDupSuppress(t *testing.T) {
	var g Group
	c := make(chan string)
	var calls int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return <-c, nil
	}

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			v, err := g.Do("key", fn)
			if err != nil {
				t.Errorf("Do error: %v", err)
			}
			if v.(string) != "bar" {
				t.Errorf("got %q; want %q", v, "bar")
			}
			wg.Done()
		}()
	}
	time.Sleep(100 * time.Millisecond) // let goroutines above block
	c <- "bar"
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

func TestDoChanDupSuppress(t *testing.T) {
	var g Group
	c := make(chan string)
	var calls int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return <-c, nil
	}

	const n = 10
	chans := make([]<-chan Result, n)
	for i := range chans {
		chans[i] = g.DoChan("key", fn)
	}
	time.Sleep(100 * time.Millisecond) // let fn start and callers attach
	c <- "bar"
	for _, ch := range chans {
		res := <-ch
		if res.Err != nil {
			t.Errorf("DoChan error: %v", res.Err)
		}
		if res.Val.(string) != "bar" {
			t.Errorf("got %q; want %q", res.Val, "bar")
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}
}

// TestDoChanAbandon verifies that a caller can stop waiting on the
// channel (e.g. on context cancellation) without blocking completion
// of the shared call or affecting other callers.
func TestDoChanAbandon(t *testing.T) {
	var g Group
	release := make(chan struct{})
	fn := func() (interface{}, error) {
		<-release
		return "bar", nil
	}

	abandoned := g.DoChan("key", fn)
	waiting := g.DoChan("key", fn)

	// The abandoning caller never receives from its channel; its
	// buffered channel must not block delivery to the other caller.
	_ = abandoned

	close(release)
	select {
	case res := <-waiting:
		if res.Err != nil || res.Val.(string) != "bar" {
			t.Errorf("waiting caller got %+v; want bar, nil", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting caller did not receive result")
	}
}
