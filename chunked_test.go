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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/golang/protobuf/proto"

	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/groupcache/singleflight"
)

func TestRangeProtoRoundTrip(t *testing.T) {
	group, key := "grp", "k"
	offset, limit := int64(4096), int64(maxFrameBytes)
	req := &pb.GetRequest{Group: &group, Key: &key, Offset: &offset, Limit: &limit}
	wire, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var decoded pb.GetRequest
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if decoded.GetOffset() != offset || decoded.GetLimit() != limit {
		t.Fatalf("range fields lost: offset=%d limit=%d", decoded.GetOffset(), decoded.GetLimit())
	}

	size := int64(123456)
	res := &pb.GetResponse{Value: []byte("frame"), Size: &size}
	wire, err = proto.Marshal(res)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var decodedRes pb.GetResponse
	if err := proto.Unmarshal(wire, &decodedRes); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if decodedRes.GetSize() != size || string(decodedRes.GetValue()) != "frame" {
		t.Fatalf("response fields lost: size=%d value=%q", decodedRes.GetSize(), decodedRes.GetValue())
	}
}

// TestHTTPChunkedRoundTrip drives the real HTTPPool server and
// httpGetter client to prove range query parameters and framed
// responses reassemble into the original object end to end.
func TestHTTPChunkedRoundTrip(t *testing.T) {
	objSize := int(maxFrameBytes*3 + 99)
	want := make([]byte, objSize)
	for i := range want {
		want[i] = byte(i*7 + 3)
	}

	mux := http.NewServeMux()
	// Construct the pool directly: NewHTTPPoolOpts is a process-wide
	// singleton and is already exercised by TestHTTPPool.
	pool := &HTTPPool{
		self: "http://peer.example/",
		opts: HTTPPoolOptions{BasePath: "/_groupcache/"},
	}
	mux.Handle(pool.opts.BasePath, pool)
	g := &Group{
		name: "http-chunked",
		getter: GetterFunc(func(_ context.Context, key string, dest Sink) error {
			return dest.SetBytes(want)
		}),
		peers:     NoPeers{},
		loadGroup: &singleflight.Group{},
	}
	groups[g.name] = g
	t.Cleanup(func() {
		mu.Lock()
		delete(groups, g.name)
		mu.Unlock()
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	getter := &httpGetter{baseURL: server.URL + "/_groupcache/"}

	got, err := g.getFromPeerChunked(context.Background(), getter, "big-key")
	if err != nil {
		t.Fatalf("getFromPeerChunked: %v", err)
	}
	if !got.EqualBytes(want) {
		t.Fatalf("reassembled object of %d bytes differs from source %d bytes", got.Len(), len(want))
	}
}

func TestHTTPServerRejectsOutOfRangeOffset(t *testing.T) {
	want := []byte("short")
	mux := http.NewServeMux()
	pool := &HTTPPool{
		self: "http://peer.example/",
		opts: HTTPPoolOptions{BasePath: "/_groupcache/"},
	}
	mux.Handle(pool.opts.BasePath, pool)
	g := &Group{
		name: "http-range-416",
		getter: GetterFunc(func(_ context.Context, key string, dest Sink) error {
			return dest.SetBytes(want)
		}),
		peers:     NoPeers{},
		loadGroup: &singleflight.Group{},
	}
	mu.Lock()
	groups[g.name] = g
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		delete(groups, g.name)
		mu.Unlock()
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/_groupcache/http-range-416/k?offset=999&limit=64")
	if err != nil {
		t.Fatalf("http.Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestedRangeNotSatisfiable)
	}
}

// fakeRangePeer serves a fixed object in range-aware frames.
type fakeRangePeer struct {
	t *testing.T

	value []byte

	// legacy makes the peer ignore range parameters and answer with
	// the whole object and no size, like a pre-upgrade server.
	legacy bool

	// failOnce maps an offset to the number of transient failures
	// returned before frames at that offset succeed.
	failOnce map[int64]int

	// maxFrame records the largest frame payload ever requested,
	// proving no frame exceeds the bounded frame size.
	maxFrame int64

	// requests records the requested (offset, limit) of every call.
	requests []frameRequest
}

type frameRequest struct {
	offset int64
	limit  int64
}

func (p *fakeRangePeer) Get(_ context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	return p.GetRange(context.Background(), in, out)
}

func (p *fakeRangePeer) GetRange(_ context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	offset, limit := in.GetOffset(), in.GetLimit()
	p.requests = append(p.requests, frameRequest{offset: offset, limit: limit})

	if p.legacy {
		out.Value = append([]byte(nil), p.value...)
		return nil
	}

	if p.failOnce != nil && p.failOnce[offset] > 0 {
		p.failOnce[offset]--
		return errors.New("simulated transient frame error")
	}

	if offset < 0 || offset > int64(len(p.value)) {
		return fmt.Errorf("offset %d out of range", offset)
	}
	if limit > maxFrameBytes {
		p.t.Errorf("requested frame limit %d exceeds maxFrameBytes %d", limit, maxFrameBytes)
	}
	if limit > p.maxFrame {
		p.maxFrame = limit
	}
	end := int64(len(p.value))
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	out.Value = append([]byte(nil), p.value[offset:end]...)
	total := int64(len(p.value))
	out.Size = &total
	return nil
}

type staticPicker struct {
	peer ProtoGetter
}

func (p staticPicker) PickPeer(string) (ProtoGetter, bool) {
	return p.peer, true
}

func newChunkedTestGroup(t *testing.T, peer ProtoGetter) *Group {
	t.Helper()
	g := &Group{
		name:      fmt.Sprintf("chunked-test-%p", t),
		peers:     staticPicker{peer: peer},
		loadGroup: &singleflight.Group{},
	}
	return g
}

func TestChunkedFetchLargeObject(t *testing.T) {
	objSize := int(maxFrameBytes*5 + 1234)
	value := make([]byte, objSize)
	for i := range value {
		value[i] = byte(i % 251)
	}
	peer := &fakeRangePeer{t: t, value: value}
	g := newChunkedTestGroup(t, peer)

	got, err := g.getFromPeerChunked(context.Background(), peer, "key")
	if err != nil {
		t.Fatalf("getFromPeerChunked: %v", err)
	}
	if got.Len() != objSize || !got.EqualBytes(value) {
		t.Fatalf("reassembled object mismatch: got %d bytes, want %d", got.Len(), objSize)
	}

	wantFrames := (int64(objSize) + maxFrameBytes - 1) / maxFrameBytes
	if int64(len(peer.requests)) != wantFrames {
		t.Fatalf("frames requested = %d, want %d", len(peer.requests), wantFrames)
	}
	if peer.requests[0].offset != 0 || peer.requests[len(peer.requests)-1].offset >= int64(objSize) {
		t.Fatalf("unexpected frame offsets: %+v", peer.requests)
	}
	if peer.maxFrame > maxFrameBytes {
		t.Fatalf("frame payload peaked at %d, cap is %d", peer.maxFrame, maxFrameBytes)
	}
}

func TestChunkedFetchRetriesFailedFrame(t *testing.T) {
	value := make([]byte, maxFrameBytes*3+7)
	peer := &fakeRangePeer{
		t:        t,
		value:    value,
		failOnce: map[int64]int{maxFrameBytes: 2},
	}
	g := newChunkedTestGroup(t, peer)

	got, err := g.getFromPeerChunked(context.Background(), peer, "key")
	if err != nil {
		t.Fatalf("getFromPeerChunked: %v", err)
	}
	if got.Len() != len(value) {
		t.Fatalf("got %d bytes, want %d", got.Len(), len(value))
	}

	// Four frames worth of requests: offsets 0 and 2*frame each once,
	// the failing offset three times (two failures then success).
	var retried int
	for _, r := range peer.requests {
		if r.offset == maxFrameBytes {
			retried++
		}
	}
	if retried != 3 {
		t.Fatalf("failed frame attempted %d times, want 3", retried)
	}
}

func TestChunkedFetchFrameExhaustsRetries(t *testing.T) {
	value := make([]byte, maxFrameBytes*2+1)
	peer := &fakeRangePeer{
		t:        t,
		value:    value,
		failOnce: map[int64]int{0: frameRetryCount + 1},
	}
	g := newChunkedTestGroup(t, peer)

	if _, err := g.getFromPeerChunked(context.Background(), peer, "key"); err == nil {
		t.Fatal("expected error after retries are exhausted, got nil")
	}
	if len(peer.requests) != frameRetryCount {
		t.Fatalf("attempts = %d, want %d", len(peer.requests), frameRetryCount)
	}
}

func TestChunkedFetchLegacyPeer(t *testing.T) {
	value := []byte("a small object from an old server")
	peer := &fakeRangePeer{t: t, value: value, legacy: true}
	g := newChunkedTestGroup(t, peer)

	got, err := g.getFromPeerChunked(context.Background(), peer, "key")
	if err != nil {
		t.Fatalf("getFromPeerChunked: %v", err)
	}
	if !got.EqualBytes(value) {
		t.Fatalf("got %q, want %q", got.String(), string(value))
	}
	if len(peer.requests) != 1 {
		t.Fatalf("legacy peer issued %d requests, want 1", len(peer.requests))
	}
}

func TestChunkedFetchEmptyObject(t *testing.T) {
	peer := &fakeRangePeer{t: t, value: nil}
	g := newChunkedTestGroup(t, peer)

	got, err := g.getFromPeerChunked(context.Background(), peer, "key")
	if err != nil {
		t.Fatalf("getFromPeerChunked: %v", err)
	}
	if got.Len() != 0 {
		t.Fatalf("got %d bytes, want 0", got.Len())
	}
}

// TestChunkedFetchBoundedMemory asserts the receiver never holds a
// second full-size copy of the object: while the fetch runs, live
// heap attributable to the transfer may grow by at most one object
// plus a few frames. It is a regression guard against reintroducing
// whole-object intermediate buffers.
func TestChunkedFetchBoundedMemory(t *testing.T) {
	objSize := int(maxFrameBytes * 320)
	value := make([]byte, objSize)
	for i := range value {
		value[i] = byte(i)
	}
	peer := &fakeRangePeer{t: t, value: value}
	g := newChunkedTestGroup(t, peer)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	got, err := g.getFromPeerChunked(context.Background(), peer, "key")
	if err != nil {
		t.Fatalf("getFromPeerChunked: %v", err)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	// Allow the result object plus 2 frames of slack for the response
	// message and proto scratch, but nowhere near a second full copy.
	limit := int64(objSize) + 2*maxFrameBytes + 1<<16
	if growth > limit {
		t.Fatalf("heap grew by %d bytes during fetch, limit %d (object %d): a second full-size copy likely exists", growth, limit, objSize)
	}
	if got.Len() != objSize {
		t.Fatalf("got %d bytes, want %d", got.Len(), objSize)
	}
}
