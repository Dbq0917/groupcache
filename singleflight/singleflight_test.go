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

// TestDoErrDupSuppress tests that a failed call's error is shared not
// only with concurrent duplicates, but also with callers (retries)
// that arrive shortly after the failure, within ErrSuppressWindow.
func TestDoErrDupSuppress(t *testing.T) {
	var g Group
	g.ErrSuppressWindow = time.Second
	someErr := errors.New("some error")
	var calls int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, someErr
	}

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, err := g.Do("key", fn); err != someErr {
				t.Errorf("Do = %v, %v; want error %v", v, err, someErr)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("number of calls = %d; want 1", got)
	}

	// A retry arriving after the failure (but within the
	// suppression window) gets the retained error instead of
	// re-invoking fn.
	if v, err := g.Do("key", fn); err != someErr {
		t.Errorf("Do = %v, %v; want error %v", v, err, someErr)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("number of calls after suppressed retry = %d; want 1", got)
	}
}

// TestDoErrSuppressExpiry tests that once ErrSuppressWindow elapses,
// the next call for the key executes fn again.
func TestDoErrSuppressExpiry(t *testing.T) {
	var g Group
	g.ErrSuppressWindow = 50 * time.Millisecond
	someErr := errors.New("some error")
	var calls int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, someErr
	}

	if _, err := g.Do("key", fn); err != someErr {
		t.Fatalf("Do error = %v; want %v", err, someErr)
	}
	if _, err := g.Do("key", fn); err != someErr {
		t.Fatalf("Do error = %v; want %v", err, someErr)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("number of calls = %d; want 1", got)
	}

	time.Sleep(200 * time.Millisecond) // let the suppression window expire
	if _, err := g.Do("key", fn); err != someErr {
		t.Fatalf("Do error = %v; want %v", err, someErr)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("number of calls after window expiry = %d; want 2", got)
	}
}

// TestDoSuccessNotRetained tests that successful calls are forgotten
// as soon as they complete: back-to-back calls for the same key each
// execute fn.
func TestDoSuccessNotRetained(t *testing.T) {
	var g Group
	var calls int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return "bar", nil
	}
	for i := 0; i < 2; i++ {
		if v, err := g.Do("key", fn); err != nil || v.(string) != "bar" {
			t.Fatalf("Do = %v, %v; want bar, nil", v, err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("number of calls = %d; want 2", got)
	}
}
