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

// Tests for groupcache.

package groupcache

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"

	pb "github.com/golang/groupcache/groupcachepb"
	testpb "github.com/golang/groupcache/testpb"
)

var (
	once                    sync.Once
	stringGroup, protoGroup Getter

	stringc = make(chan string)

	dummyCtx = context.TODO()

	// cacheFills is the number of times stringGroup or
	// protoGroup's Getter have been called. Read using the
	// cacheFills function.
	cacheFills AtomicInt
)

const (
	stringGroupName = "string-group"
	protoGroupName  = "proto-group"
	testMessageType = "google3/net/groupcache/go/test_proto.TestMessage"
	fromChan        = "from-chan"
	cacheSize       = 1 << 20
)

func testSetup() {
	stringGroup = NewGroup(stringGroupName, cacheSize, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		return dest.SetString("ECHO:" + key)
	}))

	protoGroup = NewGroup(protoGroupName, cacheSize, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		return dest.SetProto(&testpb.TestMessage{
			Name: proto.String("ECHO:" + key),
			City: proto.String("SOME-CITY"),
		})
	}))
}

// TestGetDupSuppressString tests that a Getter's Get method is only called once with two
// outstanding callers.  This is the string variant.
func TestGetDupSuppressString(t *testing.T) {
	once.Do(testSetup)
	// Start two getters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s string
			if err := stringGroup.Get(dummyCtx, fromChan, StringSink(&s)); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}
			resc <- s
		}()
	}

	// Wait a bit so both goroutines get merged together via
	// singleflight.
	// TODO(bradfitz): decide whether there are any non-offensive
	// debug/test hooks that could be added to singleflight to
	// make a sleep here unnecessary.
	time.Sleep(250 * time.Millisecond)

	// Unblock the first getter, which should unblock the second
	// as well.
	stringc <- "foo"

	for i := 0; i < 2; i++ {
		select {
		case v := <-resc:
			if v != "ECHO:foo" {
				t.Errorf("got %q; want %q", v, "ECHO:foo")
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting on getter #%d of 2", i+1)
		}
	}
}

// TestGetDupSuppressProto tests that a Getter's Get method is only called once with two
// outstanding callers.  This is the proto variant.
func TestGetDupSuppressProto(t *testing.T) {
	once.Do(testSetup)
	// Start two getters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan *testpb.TestMessage, 2)
	for i := 0; i < 2; i++ {
		go func() {
			tm := new(testpb.TestMessage)
			if err := protoGroup.Get(dummyCtx, fromChan, ProtoSink(tm)); err != nil {
				tm.Name = proto.String("ERROR:" + err.Error())
			}
			resc <- tm
		}()
	}

	// Wait a bit so both goroutines get merged together via
	// singleflight.
	// TODO(bradfitz): decide whether there are any non-offensive
	// debug/test hooks that could be added to singleflight to
	// make a sleep here unnecessary.
	time.Sleep(250 * time.Millisecond)

	// Unblock the first getter, which should unblock the second
	// as well.
	stringc <- "Fluffy"
	want := &testpb.TestMessage{
		Name: proto.String("ECHO:Fluffy"),
		City: proto.String("SOME-CITY"),
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-resc:
			if !reflect.DeepEqual(v, want) {
				t.Errorf(" Got: %v\nWant: %v", proto.CompactTextString(v), proto.CompactTextString(want))
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting on getter #%d of 2", i+1)
		}
	}
}

func countFills(f func()) int64 {
	fills0 := cacheFills.Get()
	f()
	return cacheFills.Get() - fills0
}

// uniqueGroupName returns a group name unique for the test process, so
// tests run repeatedly with -count do not hit group registration panics.
var uniqueGroupCounter int64

func uniqueGroupName(t *testing.T) string {
	return fmt.Sprintf("%s-%d", t.Name(), atomic.AddInt64(&uniqueGroupCounter, 1))
}

func TestCaching(t *testing.T) {
	once.Do(testSetup)
	fills := countFills(func() {
		for i := 0; i < 10; i++ {
			var s string
			if err := stringGroup.Get(dummyCtx, "TestCaching-key", StringSink(&s)); err != nil {
				t.Fatal(err)
			}
		}
	})
	if fills != 1 {
		t.Errorf("expected 1 cache fill; got %d", fills)
	}
}

func TestCacheEviction(t *testing.T) {
	once.Do(testSetup)
	testKey := "TestCacheEviction-key"
	getTestKey := func() {
		var res string
		for i := 0; i < 10; i++ {
			if err := stringGroup.Get(dummyCtx, testKey, StringSink(&res)); err != nil {
				t.Fatal(err)
			}
		}
	}
	fills := countFills(getTestKey)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill; got %d", fills)
	}

	g := stringGroup.(*Group)
	evict0 := g.mainCache.nevict

	// Trash the cache with other keys.
	var bytesFlooded int64
	// cacheSize/len(testKey) is approximate
	for bytesFlooded < cacheSize+1024 {
		var res string
		key := fmt.Sprintf("dummy-key-%d", bytesFlooded)
		stringGroup.Get(dummyCtx, key, StringSink(&res))
		bytesFlooded += int64(len(key) + len(res))
	}
	evicts := g.mainCache.nevict - evict0
	if evicts <= 0 {
		t.Errorf("evicts = %v; want more than 0", evicts)
	}

	// Test that the key is gone.
	fills = countFills(getTestKey)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill after cache trashing; got %d", fills)
	}
}

type fakePeer struct {
	hits    int
	deletes int
	fail    bool
}

func (p *fakePeer) Get(_ context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	p.hits++
	if p.fail {
		return errors.New("simulated error from peer")
	}
	out.Value = []byte("got:" + in.GetKey())
	return nil
}

func (p *fakePeer) Delete(_ context.Context, in *pb.GetRequest, _ *pb.GetResponse) error {
	p.deletes++
	if p.fail {
		return errors.New("simulated error from peer")
	}
	return nil
}

type fakePeers []ProtoGetter

func (p fakePeers) PickPeer(key string) (peer ProtoGetter, ok bool) {
	if len(p) == 0 {
		return
	}
	n := crc32.Checksum([]byte(key), crc32.IEEETable) % uint32(len(p))
	return p[n], p[n] != nil
}

// TestPeers tests that peers (virtual, in-process) are hit, and how much.
func TestPeers(t *testing.T) {
	once.Do(testSetup)
	peer0 := &fakePeer{}
	peer1 := &fakePeer{}
	peer2 := &fakePeer{}
	peerList := fakePeers([]ProtoGetter{peer0, peer1, peer2, nil})
	const cacheSize = 0 // disabled
	localHits := 0
	getter := func(_ context.Context, key string, dest Sink) error {
		localHits++
		return dest.SetString("got:" + key)
	}
	testGroup := newGroup("TestPeers-group", cacheSize, GetterFunc(getter), peerList)
	testGroup.rand = rand.New(rand.NewSource(123))
	run := func(name string, n int, wantSummary string) {
		// Reset counters
		localHits = 0
		for _, p := range []*fakePeer{peer0, peer1, peer2} {
			p.hits = 0
		}

		for i := 0; i < n; i++ {
			key := fmt.Sprintf("key-%d", i)
			want := "got:" + key
			var got string
			err := testGroup.Get(dummyCtx, key, StringSink(&got))
			if err != nil {
				t.Errorf("%s: error on key %q: %v", name, key, err)
				continue
			}
			if got != want {
				t.Errorf("%s: for key %q, got %q; want %q", name, key, got, want)
			}
		}
		summary := func() string {
			return fmt.Sprintf("localHits = %d, peers = %d %d %d", localHits, peer0.hits, peer1.hits, peer2.hits)
		}
		if got := summary(); got != wantSummary {
			t.Errorf("%s: got %q; want %q", name, got, wantSummary)
		}
	}
	resetCacheSize := func(maxBytes int64) {
		g := testGroup
		g.cacheBytes = maxBytes
		g.mainCache = cache{}
		g.hotCache = cache{}
	}

	// Base case; peers all up, with no problems.
	resetCacheSize(1 << 20)
	run("base", 200, "localHits = 49, peers = 51 49 51")

	// Verify cache was hit.  All localHits are gone, and some of
	// the peer hits (the ones randomly selected to be maybe hot)
	run("cached_base", 200, "localHits = 0, peers = 49 47 48")
	resetCacheSize(0)

	// With one of the peers being down.
	// TODO(bradfitz): on a peer number being unavailable, the
	// consistent hashing should maybe keep trying others to
	// spread the load out. Currently it fails back to local
	// execution if the first consistent-hash slot is unavailable.
	peerList[0] = nil
	run("one_peer_down", 200, "localHits = 100, peers = 0 49 51")

	// Failing peer
	peerList[0] = peer0
	peer0.fail = true
	run("peer0_failing", 200, "localHits = 100, peers = 51 49 51")
}

func TestTruncatingByteSliceTarget(t *testing.T) {
	var buf [100]byte
	s := buf[:]
	if err := stringGroup.Get(dummyCtx, "short", TruncatingByteSliceSink(&s)); err != nil {
		t.Fatal(err)
	}
	if want := "ECHO:short"; string(s) != want {
		t.Errorf("short key got %q; want %q", s, want)
	}

	s = buf[:6]
	if err := stringGroup.Get(dummyCtx, "truncated", TruncatingByteSliceSink(&s)); err != nil {
		t.Fatal(err)
	}
	if want := "ECHO:t"; string(s) != want {
		t.Errorf("truncated key got %q; want %q", s, want)
	}
}

func TestAllocatingByteSliceTarget(t *testing.T) {
	var dst []byte
	sink := AllocatingByteSliceSink(&dst)

	inBytes := []byte("some bytes")
	sink.SetBytes(inBytes)
	if want := "some bytes"; string(dst) != want {
		t.Errorf("SetBytes resulted in %q; want %q", dst, want)
	}
	v, err := sink.view()
	if err != nil {
		t.Fatalf("view after SetBytes failed: %v", err)
	}
	if &inBytes[0] == &dst[0] {
		t.Error("inBytes and dst share memory")
	}
	if &inBytes[0] == &v.b[0] {
		t.Error("inBytes and view share memory")
	}
	if &dst[0] == &v.b[0] {
		t.Error("dst and view share memory")
	}
}

// orderedFlightGroup allows the caller to force the schedule of when
// orig.Do will be called.  This is useful to serialize calls such
// that singleflight cannot dedup them.
type orderedFlightGroup struct {
	mu     sync.Mutex
	stage1 chan bool
	stage2 chan bool
	orig   flightGroup
}

func (g *orderedFlightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	<-g.stage1
	<-g.stage2
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.orig.Do(key, fn)
}

// TestNoDedup tests invariants on the cache size when singleflight is
// unable to dedup calls.
func TestNoDedup(t *testing.T) {
	const testkey = "testkey"
	const testval = "testval"
	g := newGroup("testgroup", 1024, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		return dest.SetString(testval)
	}), nil)

	orderedGroup := &orderedFlightGroup{
		stage1: make(chan bool),
		stage2: make(chan bool),
		orig:   g.loadGroup,
	}
	// Replace loadGroup with our wrapper so we can control when
	// loadGroup.Do is entered for each concurrent request.
	g.loadGroup = orderedGroup

	// Issue two idential requests concurrently.  Since the cache is
	// empty, it will miss.  Both will enter load(), but we will only
	// allow one at a time to enter singleflight.Do, so the callback
	// function will be called twice.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s string
			if err := g.Get(dummyCtx, testkey, StringSink(&s)); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}
			resc <- s
		}()
	}

	// Ensure both goroutines have entered the Do routine.  This implies
	// both concurrent requests have checked the cache, found it empty,
	// and called load().
	orderedGroup.stage1 <- true
	orderedGroup.stage1 <- true
	orderedGroup.stage2 <- true
	orderedGroup.stage2 <- true

	for i := 0; i < 2; i++ {
		if s := <-resc; s != testval {
			t.Errorf("result is %s want %s", s, testval)
		}
	}

	const wantItems = 1
	if g.mainCache.items() != wantItems {
		t.Errorf("mainCache has %d items, want %d", g.mainCache.items(), wantItems)
	}

	// If the singleflight callback doesn't double-check the cache again
	// upon entry, we would increment nbytes twice but the entry would
	// only be in the cache once.
	const wantBytes = int64(len(testkey) + len(testval))
	if g.mainCache.nbytes != wantBytes {
		t.Errorf("cache has %d bytes, want %d", g.mainCache.nbytes, wantBytes)
	}
}

func TestGroupStatsAlignment(t *testing.T) {
	var g Group
	off := unsafe.Offsetof(g.Stats)
	if off%8 != 0 {
		t.Fatal("Stats structure is not 8-byte aligned.")
	}
}

// TestDeleteNotCountedAsEviction verifies that an explicit Delete removes
// the entry without being counted as a capacity eviction.
func TestDeleteNotCountedAsEviction(t *testing.T) {
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		return dest.SetString("ECHO:" + key)
	}), NoPeers{})

	var s string
	if err := g.Get(dummyCtx, "key", StringSink(&s)); err != nil {
		t.Fatal(err)
	}
	if g.mainCache.items() != 1 {
		t.Fatalf("mainCache has %d items, want 1", g.mainCache.items())
	}

	if err := g.Delete(dummyCtx, "key"); err != nil {
		t.Fatal(err)
	}
	if items := g.mainCache.items(); items != 0 {
		t.Errorf("mainCache has %d items after Delete, want 0", items)
	}
	if evicts := g.mainCache.nevict; evicts != 0 {
		t.Errorf("Delete incremented nevict to %d, want 0", evicts)
	}

	// Capacity pressure still counts as evictions.
	var flooded int64
	for g.mainCache.nevict == 0 {
		key := fmt.Sprintf("dummy-%d", flooded)
		if err := g.Get(dummyCtx, key, StringSink(&s)); err != nil {
			t.Fatal(err)
		}
		flooded++
		if flooded > 100000 {
			t.Fatal("never saw an eviction")
		}
	}
}

// TestDeleteForcesReload verifies that after Delete the next Get does not
// serve the stale cached value but calls the getter again.
func TestDeleteForcesReload(t *testing.T) {
	var fills AtomicInt
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		fills.Add(1)
		return dest.SetString("ECHO:" + key)
	}), NoPeers{})

	get := func() string {
		var s string
		if err := g.Get(dummyCtx, "key", StringSink(&s)); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if got := get(); got != "ECHO:key" {
		t.Fatalf("got %q", got)
	}
	if get(); fills.Get() != 1 {
		t.Fatalf("fills = %d, want 1", fills.Get())
	}

	if err := g.Delete(dummyCtx, "key"); err != nil {
		t.Fatal(err)
	}
	if get(); fills.Get() != 2 {
		t.Errorf("fills = %d after Delete, want 2", fills.Get())
	}
}

// TestDeleteDropsHotAndMain verifies Delete clears both caches, including
// a mirrored hotCache copy held by a non-owner.
func TestDeleteDropsHotAndMain(t *testing.T) {
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		return dest.SetString("ECHO:" + key)
	}), NoPeers{})

	key := "TestDeleteDropsHotAndMain-key"
	view := ByteView{s: "ECHO:" + key}
	g.mainCache.add(key, view)
	g.hotCache.add(key, view)

	if err := g.Delete(dummyCtx, key); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.mainCache.get(key); ok {
		t.Error("key still present in mainCache after Delete")
	}
	if _, ok := g.hotCache.get(key); ok {
		t.Error("key still present in hotCache after Delete")
	}
	if g.mainCache.nevict != 0 || g.hotCache.nevict != 0 {
		t.Errorf("nevict main=%d hot=%d, want 0/0", g.mainCache.nevict, g.hotCache.nevict)
	}
}

// TestDeleteDuringInFlightLoad verifies that a load already running when
// Delete is called cannot write its (stale) result back into the cache.
func TestDeleteDuringInFlightLoad(t *testing.T) {
	release := make(chan struct{})
	var getterOnce sync.Once
	var fills AtomicInt
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		fills.Add(1)
		getterOnce.Do(func() { <-release })
		return dest.SetString("ECHO:" + key)
	}), NoPeers{})

	key := "TestDeleteDuringInFlightLoad-key"
	loadDone := make(chan struct{})
	go func() {
		var s string
		if err := g.Get(dummyCtx, key, StringSink(&s)); err != nil {
			t.Error(err)
		}
		close(loadDone)
	}()

	// Wait until the in-flight getter is blocked.
	deadline := time.Now().Add(5 * time.Second)
	for fills.Get() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fills.Get() == 0 {
		t.Fatal("getter never started")
	}

	if err := g.Delete(dummyCtx, key); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-loadDone

	if _, ok := g.mainCache.get(key); ok {
		t.Fatal("in-flight load wrote the deleted key back into mainCache")
	}

	// The next Get must reload rather than hit the stale value.
	var s string
	if err := g.Get(dummyCtx, key, StringSink(&s)); err != nil {
		t.Fatal(err)
	}
	if fills.Get() != 2 {
		t.Errorf("fills = %d, want 2 (in-flight load + reload)", fills.Get())
	}
}

// TestDeleteForwardsToOwner verifies a non-owner forwards Delete to the
// key's owner instead of only dropping its local mirror.
func TestDeleteForwardsToOwner(t *testing.T) {
	peer0 := &fakePeer{}
	peer1 := &fakePeer{}
	// A nil slot represents this node, mirroring TestPeers.
	peerList := fakePeers([]ProtoGetter{peer0, peer1, nil})
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		return dest.SetString("ECHO:" + key)
	}), peerList)

	// Find a key whose owner is a remote peer rather than this node.
	var key string
	var owner *fakePeer
	for i := 0; i < 100000; i++ {
		key = fmt.Sprintf("delete-key-%d", i)
		peer, ok := peerList.PickPeer(key)
		if ok {
			owner = peer.(*fakePeer)
			break
		}
	}
	if owner == nil {
		t.Fatal("no remote-owned key found")
	}

	// Seed a local hotCache mirror that Delete must also clear.
	view := ByteView{s: "stale:" + key}
	g.hotCache.add(key, view)

	if err := g.Delete(dummyCtx, key); err != nil {
		t.Fatal(err)
	}
	if owner.deletes != 1 {
		t.Errorf("owner received %d Delete calls, want 1", owner.deletes)
	}
	if _, ok := g.hotCache.get(key); ok {
		t.Error("local hotCache mirror survived Delete")
	}

	// A failing owner must surface the error and leave the local copy alone
	// so the failure is visible rather than silently half-deleting.
	owner.fail = true
	g.hotCache.add(key, view)
	if err := g.Delete(dummyCtx, key); err == nil {
		t.Error("Delete to failing owner returned nil, want error")
	}
	if _, ok := g.hotCache.get(key); !ok {
		t.Error("local copy was dropped despite failed forward")
	}
}

// TestDeleteOnOwnerNoForward verifies Delete stays local when this node
// owns the key, and that a reloaded key consumes its invalidation marker.
func TestDeleteOnOwnerNoForward(t *testing.T) {
	peer0 := &fakePeer{}
	peer1 := &fakePeer{}
	peerList := fakePeers([]ProtoGetter{peer0, peer1, nil})
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		return dest.SetString("ECHO:" + key)
	}), peerList)

	var key string
	found := false
	for i := 0; i < 100000; i++ {
		key = fmt.Sprintf("owner-key-%d", i)
		if _, ok := peerList.PickPeer(key); !ok {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no owner-local key found")
	}

	var s string
	if err := g.Get(dummyCtx, key, StringSink(&s)); err != nil {
		t.Fatal(err)
	}
	if err := g.Delete(dummyCtx, key); err != nil {
		t.Fatal(err)
	}
	if peer0.deletes+peer1.deletes != 0 {
		t.Errorf("Delete forwarded to peer for owner-local key: %d/%d", peer0.deletes, peer1.deletes)
	}

	// Reloading must consume the marker, and a later Delete still works.
	if err := g.Get(dummyCtx, key, StringSink(&s)); err != nil {
		t.Fatal(err)
	}
	g.invalidateMu.Lock()
	_, marked := g.invalidations[key]
	g.invalidateMu.Unlock()
	if marked {
		t.Error("invalidation marker survived the reload")
	}
	if err := g.Delete(dummyCtx, key); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.mainCache.get(key); ok {
		t.Error("second Delete did not clear mainCache")
	}
}

// TODO(bradfitz): port the Google-internal full integration test into here,
// using HTTP requests instead of our RPC system.
