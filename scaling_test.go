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

package groupcache

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang/groupcache/consistenthash"
	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
)

var scalingGroupID int

type scalingTestPeer struct {
	fn func(context.Context, *pb.GetRequest, *pb.GetResponse) error
}

func (p *scalingTestPeer) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	return p.fn(ctx, in, out)
}

type scalingTestNode struct {
	name   string
	picker *scalingTestPicker
	group  *Group
	fills  AtomicInt
}

func (n *scalingTestNode) serve(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	var value ByteView
	if err := n.group.Get(ctx, in.GetKey(), ByteViewSink(&value)); err != nil {
		return err
	}
	out.Value = value.ByteSlice()
	out.RingVersion = proto.Uint64(n.picker.RingVersion())
	out.ValueRingVersion = proto.Uint64(value.version.ring)
	out.ValueVersion = proto.Uint64(value.version.seq)
	return nil
}

type scalingTestPicker struct {
	self string

	mu          sync.RWMutex
	version     uint64
	ring        *consistenthash.Map
	prevRing    *consistenthash.Map
	getters     map[string]ProtoGetter
	prevGetters map[string]ProtoGetter
}

func newScalingTestPicker(self string) *scalingTestPicker {
	return &scalingTestPicker{
		self:        self,
		ring:        consistenthash.New(50, nil),
		getters:     make(map[string]ProtoGetter),
		prevGetters: make(map[string]ProtoGetter),
	}
}

func (p *scalingTestPicker) setRing(version uint64, nodes map[string]*scalingTestNode, names []string) {
	p.setRingWithPrevious(version, nodes, names, nil)
}

func (p *scalingTestPicker) setRingWithPrevious(version uint64, nodes map[string]*scalingTestNode, names, prevNames []string) {
	ring := consistenthash.New(50, nil)
	ring.Add(names...)
	getters := make(map[string]ProtoGetter, len(names))
	for _, name := range names {
		node := nodes[name]
		getters[name] = &scalingTestPeer{fn: node.serve}
	}

	p.mu.Lock()
	if prevNames == nil {
		p.prevRing = p.ring
		p.prevGetters = p.getters
	} else {
		p.prevRing = consistenthash.New(50, nil)
		p.prevRing.Add(prevNames...)
		p.prevGetters = make(map[string]ProtoGetter, len(prevNames))
		for _, name := range prevNames {
			node := nodes[name]
			p.prevGetters[name] = &scalingTestPeer{fn: node.serve}
		}
	}
	p.version = version
	p.ring = ring
	p.getters = getters
	p.mu.Unlock()
}

func (p *scalingTestPicker) RingVersion() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.version
}

func (p *scalingTestPicker) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	owner := p.ring.Get(key)
	if owner == "" || owner == p.self {
		return nil, false
	}
	peer, ok := p.getters[owner]
	return peer, ok
}

func (p *scalingTestPicker) PickPreviousOwner(key string) (ProtoGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.prevRing == nil {
		return nil, false
	}
	prev := p.prevRing.Get(key)
	if prev == "" || prev == p.self || prev == p.ring.Get(key) {
		return nil, false
	}
	peer, ok := p.prevGetters[prev]
	return peer, ok
}

func (p *scalingTestPicker) owners(key string) (current, previous string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	current = p.ring.Get(key)
	if p.prevRing != nil {
		previous = p.prevRing.Get(key)
	}
	return
}

func newScalingTestCluster(t *testing.T, names ...string) map[string]*scalingTestNode {
	t.Helper()
	nodes := make(map[string]*scalingTestNode, len(names))
	for _, name := range names {
		node := &scalingTestNode{name: name, picker: newScalingTestPicker(name)}
		nodes[name] = node
	}
	for _, name := range names {
		node := nodes[name]
		scalingGroupID++
		node.group = newGroup(
			fmt.Sprintf("%s-scaling-%d", t.Name(), scalingGroupID),
			1<<20,
			GetterFunc(func(_ context.Context, key string, dest Sink) error {
				node.fills.Add(1)
				return dest.SetString("value:" + key)
			}),
			node.picker,
		)
	}
	return nodes
}

func TestScaleExpansionReadsCompareBeforeAfter(t *testing.T) {
	nodes := newScalingTestCluster(t, "a", "b", "c", "d")
	beforeRing := []string{"a", "b", "c"}
	afterRing := []string{"a", "b", "c", "d"}
	for _, name := range beforeRing {
		nodes[name].picker.setRing(1, nodes, beforeRing)
	}

	const numKeys = 600
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("scale-key-%04d", i)
		var got string
		if err := nodes["a"].group.Get(context.Background(), key, StringSink(&got)); err != nil {
			t.Fatalf("pre-expansion Get(%q): %v", key, err)
		}
		before[key] = got
	}

	for _, name := range afterRing {
		if name == "d" {
			// A newly joined peer learns the previous membership from
			// the ring state it pulls from an existing peer.
			nodes[name].picker.setRingWithPrevious(2, nodes, afterRing, beforeRing)
		} else {
			nodes[name].picker.setRing(2, nodes, afterRing)
		}
	}

	oldOwnersServingHandoff := make(map[string]bool)
	moved := 0
	var sampleBefore, sampleAfter string
	for key, want := range before {
		cur, prev := nodes["a"].picker.owners(key)
		if prev == cur {
			continue
		}
		moved++
		oldOwnersServingHandoff[prev] = true

		var got string
		if err := nodes["a"].group.Get(context.Background(), key, StringSink(&got)); err != nil {
			t.Fatalf("post-expansion Get(%q): %v", key, err)
		}
		if got != want {
			t.Fatalf("post-expansion Get(%q) = %q, want %q", key, got, want)
		}
		if sampleBefore == "" {
			sampleBefore, sampleAfter = want, got
		}
	}
	if moved == 0 {
		t.Fatal("expansion did not move any keys")
	}
	if got := nodes["d"].group.Stats.HandoffLoads.Get(); got != int64(moved) {
		t.Errorf("new owner handoff loads = %d, want %d", got, moved)
	}
	if got := nodes["d"].fills.Get(); got != 0 {
		t.Errorf("new owner made %d backing-store fills, want 0 (all values should be handed off)", got)
	}
	if len(oldOwnersServingHandoff) < 2 {
		t.Errorf("migration was concentrated on %d old owner(s), want at least 2", len(oldOwnersServingHandoff))
	}
	t.Logf("checked %d keys: moved=%d old-owners=%d new-owner-handoffs=%d new-owner-fills=%d sample=%q->%q",
		len(before), moved, len(oldOwnersServingHandoff),
		nodes["d"].group.Stats.HandoffLoads.Get(), nodes["d"].fills.Get(),
		sampleBefore, sampleAfter)
}

func TestScaleHandoffReadDoesNotWriteOldOwner(t *testing.T) {
	scalingGroupID++
	var fills int
	g := newGroup(
		fmt.Sprintf("%s-%d", t.Name(), scalingGroupID),
		1<<20,
		GetterFunc(func(_ context.Context, key string, dest Sink) error {
			fills++
			return dest.SetString("old:" + key)
		}),
		NoPeers{},
	)

	var got string
	if err := g.Get(withHandoff(context.Background()), "k", StringSink(&got)); err != nil {
		t.Fatal(err)
	}
	if got != "old:k" {
		t.Fatalf("handoff Get = %q, want %q", got, "old:k")
	}
	if fills != 1 {
		t.Fatalf("old owner made %d fills, want 1", fills)
	}
	if items := g.mainCache.items(); items != 0 {
		t.Fatalf("old owner cached %d handoff values, want 0", items)
	}
}

func TestScaleNewerLocalValueWinsOverHandoff(t *testing.T) {
	scalingGroupID++
	prevCalled := false
	prev := &scalingTestPeer{fn: func(_ context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
		prevCalled = true
		out.Value = []byte("old:" + in.GetKey())
		return nil
	}}
	picker := &scalingVersionedHandoffPicker{
		version: 2,
		prev:    prev,
		prevOK:  true,
	}
	g := newGroup(
		fmt.Sprintf("%s-%d", t.Name(), scalingGroupID),
		1<<20,
		GetterFunc(func(_ context.Context, key string, dest Sink) error {
			return dest.SetString("new:" + key)
		}),
		picker,
	)
	g.populateCache("k", ByteView{
		s:       "new:k",
		version: valueVersion{ring: 2, seq: 10},
	}, &g.mainCache)

	var got string
	if err := g.Get(context.Background(), "k", StringSink(&got)); err != nil {
		t.Fatal(err)
	}
	if got != "new:k" {
		t.Fatalf("Get = %q, want newer local value %q", got, "new:k")
	}
	if prevCalled {
		t.Error("read fetched the stale previous owner despite a newer local value")
	}
}

type scalingVersionedHandoffPicker struct {
	version uint64
	prev    ProtoGetter
	prevOK  bool
}

func (p *scalingVersionedHandoffPicker) PickPeer(string) (ProtoGetter, bool) {
	return nil, false
}

func (p *scalingVersionedHandoffPicker) PickPreviousOwner(string) (ProtoGetter, bool) {
	return p.prev, p.prevOK
}

func (p *scalingVersionedHandoffPicker) RingVersion() uint64 {
	return p.version
}

func TestScaleTransitionWindowExpires(t *testing.T) {
	p := &HTTPPool{
		self:        "http://a",
		httpGetters: make(map[string]*httpGetter),
		opts: HTTPPoolOptions{
			BasePath:         defaultBasePath,
			Replicas:         defaultReplicas,
			TransitionWindow: 20 * time.Millisecond,
		},
		peers: consistenthash.New(defaultReplicas, nil),
	}
	p.SetWithVersion(1, "http://a", "http://b")
	p.SetWithVersion(2, "http://a", "http://b", "http://c")
	time.Sleep(30 * time.Millisecond)
	if _, ok := p.PickPreviousOwner("any-key"); ok {
		t.Error("handoff remained enabled after the transition window")
	}
}

func TestScaleRingStateCarriesPreviousMembership(t *testing.T) {
	newPool := func(self string) *HTTPPool {
		p := &HTTPPool{
			self:        self,
			httpGetters: make(map[string]*httpGetter),
			opts: HTTPPoolOptions{
				BasePath:         defaultBasePath,
				Replicas:         defaultReplicas,
				TransitionWindow: time.Minute,
			},
			peers: consistenthash.New(defaultReplicas, nil),
		}
		return p
	}

	oldPeers := []string{"http://a", "http://b", "http://c"}
	newPeers := []string{"http://a", "http://b", "http://c", "http://d"}
	source := newPool("http://a")
	source.SetWithVersion(1, oldPeers...)
	source.SetWithVersion(2, newPeers...)
	srv := httptest.NewServer(source)
	defer srv.Close()

	joined := newPool("http://d")
	joined.SetWithVersion(1, oldPeers...)
	if err := joined.syncRingFrom(context.Background(), srv.URL+defaultBasePath); err != nil {
		t.Fatal(err)
	}
	if got := joined.RingVersion(); got != 2 {
		t.Fatalf("joined ring version = %d, want 2", got)
	}

	found := false
	for i := 0; i < 1000; i++ {
		if _, ok := joined.PickPreviousOwner(fmt.Sprintf("ring-state-key-%d", i)); ok {
			found = true
			break
		}
	}
	if !found {
		t.Error("joined peer did not receive a usable previous ring")
	}
}
