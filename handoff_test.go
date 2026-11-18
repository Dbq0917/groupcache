/*
Copyright 2026 Google Inc.

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

// Tests for versioned ring updates, ring anti-entropy and handoff reads.

package groupcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/groupcache/consistenthash"
	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
)

// newTestPool builds an HTTPPool without touching the package-level
// registry, so tests can create multiple pools in one process.
func newTestPool(self string, opts *HTTPPoolOptions) *HTTPPool {
	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if opts != nil {
		p.opts = *opts
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	return p
}

func TestSetWithVersion(t *testing.T) {
	p := newTestPool("http://a", nil)

	if !p.SetWithVersion(5, "http://a", "http://b") {
		t.Fatal("SetWithVersion(5) rejected on a fresh pool")
	}
	if v := p.RingVersion(); v != 5 {
		t.Fatalf("RingVersion = %d, want 5", v)
	}
	if p.SetWithVersion(5, "http://a") {
		t.Error("SetWithVersion(5) accepted an equal version")
	}
	if p.SetWithVersion(4, "http://a") {
		t.Error("SetWithVersion(4) accepted a stale version")
	}
	if v := p.RingVersion(); v != 5 {
		t.Fatalf("RingVersion = %d after stale updates, want 5", v)
	}
	if !p.SetWithVersion(6, "http://a", "http://b", "http://c") {
		t.Error("SetWithVersion(6) rejected a newer version")
	}

	// The legacy unversioned Set always applies and does not
	// advance the ring version.
	q := newTestPool("http://a", nil)
	q.Set("http://a", "http://b")
	if v := q.RingVersion(); v != 0 {
		t.Fatalf("RingVersion = %d after legacy Set, want 0", v)
	}
	if !q.SetWithVersion(3, "http://a") {
		t.Error("SetWithVersion(3) rejected after legacy Sets")
	}
}

func TestRingStateEndpoint(t *testing.T) {
	p := newTestPool("http://a", nil)
	p.SetWithVersion(7, "http://a", "http://b")
	srv := httptest.NewServer(p)
	defer srv.Close()

	res, err := http.Get(srv.URL + p.opts.BasePath + ringStatePath)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("ring state status = %v", res.Status)
	}
	if got := res.Header.Get(ringVersionHeader); got != "7" {
		t.Errorf("ring version header = %q, want %q", got, "7")
	}
	var state ringState
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state.Version != 7 {
		t.Errorf("state.Version = %d, want 7", state.Version)
	}
	wantPeers := []string{"http://a", "http://b"}
	if !reflect.DeepEqual(state.Peers, wantPeers) {
		t.Errorf("state.Peers = %v, want %v", state.Peers, wantPeers)
	}

	// Ordinary responses carry the ring version too, even errors.
	res2, err := http.Get(srv.URL + p.opts.BasePath)
	if err != nil {
		t.Fatal(err)
	}
	res2.Body.Close()
	if got := res2.Header.Get(ringVersionHeader); got != "7" {
		t.Errorf("ring version header on error response = %q, want %q", got, "7")
	}
}

func TestRingSyncFromPeer(t *testing.T) {
	poolA := newTestPool("self-a", nil)
	srvA := httptest.NewServer(poolA)
	defer srvA.Close()
	poolA.self = srvA.URL

	poolB := newTestPool("self-b", nil)
	srvB := httptest.NewServer(poolB)
	defer srvB.Close()
	poolB.self = srvB.URL

	// B runs an old ring; A runs a newer one.
	poolA.SetWithVersion(2, srvA.URL, srvB.URL)
	poolB.SetWithVersion(1, srvA.URL)

	// B talks to A and observes A's newer ring version in the
	// response header; that must trigger a pull of A's ring state.
	getter := poolB.httpGetters[srvA.URL]
	if getter == nil {
		t.Fatal("poolB has no getter for A")
	}
	var out pb.GetResponse
	in := &pb.GetRequest{Group: proto.String("no-such-group"), Key: proto.String("k")}
	if err := getter.Get(context.Background(), in, &out); err == nil {
		t.Log("unexpected success for unknown group; sync is triggered either way")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if poolB.RingVersion() == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("poolB did not converge: RingVersion = %d, want 2", poolB.RingVersion())
		}
		time.Sleep(10 * time.Millisecond)
	}
	poolB.mu.Lock()
	gotPeers := append([]string(nil), poolB.peerList...)
	poolB.mu.Unlock()
	wantPeers := []string{srvA.URL, srvB.URL}
	if !reflect.DeepEqual(gotPeers, wantPeers) {
		t.Errorf("poolB peers after sync = %v, want %v", gotPeers, wantPeers)
	}
}

func TestPickPreviousOwner(t *testing.T) {
	p := newTestPool("http://a", &HTTPPoolOptions{TransitionWindow: time.Minute})
	p.SetWithVersion(1, "http://a", "http://b")
	p.SetWithVersion(2, "http://a", "http://b", "http://c")

	var movedKey, stableKey string
	for i := 0; i < 10000 && (movedKey == "" || stableKey == ""); i++ {
		key := fmt.Sprintf("key-%d", i)
		p.mu.Lock()
		prev, cur := p.prevPeers.Get(key), p.peers.Get(key)
		p.mu.Unlock()
		switch {
		case prev != cur && prev != p.self && movedKey == "":
			movedKey = key
		case prev == cur && stableKey == "":
			stableKey = key
		}
	}
	if movedKey == "" || stableKey == "" {
		t.Fatalf("could not find moved and stable keys (moved=%q stable=%q)", movedKey, stableKey)
	}

	peer, ok := p.PickPreviousOwner(movedKey)
	if !ok {
		t.Fatalf("PickPreviousOwner(%q) = false, want previous owner", movedKey)
	}
	p.mu.Lock()
	wantGetter := p.prevGetters[p.prevPeers.Get(movedKey)]
	p.mu.Unlock()
	if peer != wantGetter {
		t.Errorf("PickPreviousOwner(%q) returned wrong getter", movedKey)
	}

	if _, ok := p.PickPreviousOwner(stableKey); ok {
		t.Errorf("PickPreviousOwner(%q) = true for a key whose ownership did not change", stableKey)
	}

	// After the window expires, no handoff is offered and the
	// previous ring is released.
	p.mu.Lock()
	p.transitionTo = time.Now().Add(-time.Second)
	p.mu.Unlock()
	if _, ok := p.PickPreviousOwner(movedKey); ok {
		t.Error("PickPreviousOwner after window expiry = true, want false")
	}
	p.mu.Lock()
	expired := p.prevPeers == nil
	p.mu.Unlock()
	if !expired {
		t.Error("previous ring not released after window expiry")
	}
}

type fakeProtoGetter struct {
	calls int32
	fn    func(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error
}

func (f *fakeProtoGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	atomic.AddInt32(&f.calls, 1)
	return f.fn(ctx, in, out)
}

type fakeHandoffPicker struct {
	peer   ProtoGetter
	peerOK bool
	prev   ProtoGetter
	prevOK bool
}

func (f *fakeHandoffPicker) PickPeer(key string) (ProtoGetter, bool) { return f.peer, f.peerOK }
func (f *fakeHandoffPicker) PickPreviousOwner(key string) (ProtoGetter, bool) {
	return f.prev, f.prevOK
}

func TestLoadHandoffFromPreviousOwner(t *testing.T) {
	prev := &fakeProtoGetter{fn: func(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
		if !isHandoff(ctx) {
			t.Error("handoff fetch context is missing the handoff marker")
		}
		out.Value = []byte("from-prev:" + in.GetKey())
		return nil
	}}
	picker := &fakeHandoffPicker{prev: prev, prevOK: true}

	var getterCalls int32
	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		atomic.AddInt32(&getterCalls, 1)
		return dest.SetString("from-getter:" + key)
	})
	g := newGroup("handoff-load-test", cacheSize, getter, picker)

	var v string
	if err := g.Get(context.Background(), "key1", StringSink(&v)); err != nil {
		t.Fatal(err)
	}
	if v != "from-prev:key1" {
		t.Errorf("Get = %q, want %q", v, "from-prev:key1")
	}
	if n := atomic.LoadInt32(&getterCalls); n != 0 {
		t.Errorf("getter called %d times during handoff, want 0", n)
	}
	if n := g.Stats.HandoffLoads.Get(); n != 1 {
		t.Errorf("HandoffLoads = %d, want 1", n)
	}

	// The value is now in mainCache, so a second Get is a local hit.
	if err := g.Get(context.Background(), "key1", StringSink(&v)); err != nil {
		t.Fatal(err)
	}
	if v != "from-prev:key1" {
		t.Errorf("second Get = %q, want %q", v, "from-prev:key1")
	}
	if n := atomic.LoadInt32(&prev.calls); n != 1 {
		t.Errorf("previous owner called %d times, want 1 (second Get should hit mainCache)", n)
	}

	// A failing handoff falls back to the local Getter.
	prevErr := &fakeProtoGetter{fn: func(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
		return errors.New("previous owner unreachable")
	}}
	errPicker := &fakeHandoffPicker{prev: prevErr, prevOK: true}
	g2 := newGroup("handoff-error-test", cacheSize, getter, errPicker)
	if err := g2.Get(context.Background(), "key2", StringSink(&v)); err != nil {
		t.Fatal(err)
	}
	if v != "from-getter:key2" {
		t.Errorf("Get after handoff failure = %q, want %q", v, "from-getter:key2")
	}
	if n := g2.Stats.HandoffErrors.Get(); n != 1 {
		t.Errorf("HandoffErrors = %d, want 1", n)
	}
}

func TestHandoffRequestIsTerminal(t *testing.T) {
	// A request arriving with the handoff marker must be served
	// locally even if the ring points elsewhere; otherwise two
	// peers that disagree about ownership could forward the same
	// key to each other forever.
	peer := &fakeProtoGetter{fn: func(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
		out.Value = []byte("from-peer")
		return nil
	}}
	picker := &fakeHandoffPicker{peer: peer, peerOK: true}

	var getterCalls int32
	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		atomic.AddInt32(&getterCalls, 1)
		return dest.SetString("from-getter:" + key)
	})
	g := newGroup("handoff-terminal-test", cacheSize, getter, picker)

	var v string
	if err := g.Get(withHandoff(context.Background()), "key1", StringSink(&v)); err != nil {
		t.Fatal(err)
	}
	if v != "from-getter:key1" {
		t.Errorf("handoff Get = %q, want %q", v, "from-getter:key1")
	}
	if n := atomic.LoadInt32(&peer.calls); n != 0 {
		t.Errorf("peer called %d times for a handoff request, want 0", n)
	}

	// Sanity check: without the marker the same key is forwarded.
	if err := g.Get(context.Background(), "key2", StringSink(&v)); err != nil {
		t.Fatal(err)
	}
	if v != "from-peer" {
		t.Errorf("ordinary Get = %q, want %q", v, "from-peer")
	}
}

func TestHandoffQueryParamSetsContext(t *testing.T) {
	p := newTestPool("http://self", nil)
	srv := httptest.NewServer(p)
	defer srv.Close()

	sawHandoff := make(chan bool, 1)
	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		sawHandoff <- isHandoff(ctx)
		return dest.SetString("v")
	})
	NewGroup("handoff-http-test", cacheSize, getter)

	url := srv.URL + p.opts.BasePath + "handoff-http-test/somekey?handoff=1"
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %v", res.Status)
	}
	if !<-sawHandoff {
		t.Error("getter did not observe the handoff marker for ?handoff=1")
	}
}
