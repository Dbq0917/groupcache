/*
Copyright 2013 Google Inc.

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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/groupcache/consistenthash"
	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// ringVersionHeader carries the serving peer's ring version on every
// response, so that peers can detect a newer ring and pull it
// immediately instead of waiting for the next broadcast.
const ringVersionHeader = "X-Groupcache-Ring-Version"

// ringStatePath is the reserved path (relative to BasePath) that
// serves the pool's current ring version and peer list.
const ringStatePath = "_ring"

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, the server uses the request's context
	Context func(*http.Request) context.Context

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper

	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	// opts specifies the options.
	opts HTTPPoolOptions

	mu           sync.Mutex // guards the fields below
	peers        *consistenthash.Map
	httpGetters  map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
	ringVersion  uint64
	peerList     []string
	prevPeers    *consistenthash.Map    // previous ring, kept during the transition window
	prevGetters  map[string]*httpGetter // getters for the previous peer set
	transitionTo time.Time              // handoff window deadline

	ringSyncing int32 // atomic: non-zero while a ring pull is in flight
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash

	// TransitionWindow specifies how long after a ring update the
	// previous ring is kept for handoff reads. While the window is
	// open, keys that moved to a new owner are fetched from their
	// previous owner's cache on a local miss, instead of being loaded
	// from the underlying Getter. If zero, handoff is disabled and
	// ring updates take effect immediately (the historical behavior).
	TransitionWindow time.Duration
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
//
// Set preserves the historical unversioned behavior: it always
// applies and does not advance the ring version. New deployments
// should prefer SetWithVersion so that stale broadcasts cannot
// overwrite a newer ring and peers can detect divergence.
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setLocked(p.ringVersion, peers)
}

// SetWithVersion updates the pool's list of peers and records version
// as the ring's version. The update is rejected (returning false) if
// version is not greater than the current ring version, which makes
// out-of-order or stale broadcasts harmless. Versions should be
// assigned by a single authority (e.g. the configuration system),
// for example using a Unix timestamp.
func (p *HTTPPool) SetWithVersion(version uint64, peers ...string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if version <= p.ringVersion {
		return false
	}
	p.setLocked(version, peers)
	return true
}

func (p *HTTPPool) setLocked(version uint64, peers []string) {
	if p.opts.TransitionWindow > 0 && !p.peers.IsEmpty() {
		p.prevPeers = p.peers
		p.prevGetters = p.httpGetters
		p.transitionTo = time.Now().Add(p.opts.TransitionWindow)
	} else {
		p.prevPeers = nil
		p.prevGetters = nil
		p.transitionTo = time.Time{}
	}
	p.ringVersion = version
	p.peerList = append([]string(nil), peers...)
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{transport: p.Transport, baseURL: peer + p.opts.BasePath, pool: p}
	}
}

// RingVersion returns the version of the currently installed ring.
// It is 0 if the ring was only ever set with the unversioned Set.
func (p *HTTPPool) RingVersion() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ringVersion
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

// PickPreviousOwner implements HandoffPicker. While the transition
// window is open, it returns the key's owner under the previous ring
// if ownership of the key changed with the current ring.
func (p *HTTPPool) PickPreviousOwner(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.prevPeers == nil || p.prevPeers.IsEmpty() {
		return nil, false
	}
	if time.Now().After(p.transitionTo) {
		p.prevPeers = nil
		p.prevGetters = nil
		p.transitionTo = time.Time{}
		return nil, false
	}
	prev := p.prevPeers.Get(key)
	if prev == "" || prev == p.self {
		return nil, false
	}
	if p.peers.Get(key) == prev {
		// Ownership did not change; nothing to hand off.
		return nil, false
	}
	getter, ok := p.prevGetters[prev]
	if !ok {
		return nil, false
	}
	return getter, true
}

// ringState is the JSON document served by and fetched from the
// ringStatePath endpoint.
type ringState struct {
	Version uint64   `json:"version"`
	Peers   []string `json:"peers"`
}

func (p *HTTPPool) serveRingState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	p.mu.Lock()
	state := ringState{Version: p.ringVersion, Peers: append([]string(nil), p.peerList...)}
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(ringVersionHeader, strconv.FormatUint(state.Version, 10))
	json.NewEncoder(w).Encode(state)
}

// observePeerVersion records a ring version seen on a peer's
// response. If the peer runs a newer ring, one goroutine is started
// to pull the full ring state from it, so the pool converges within
// a single request round-trip instead of waiting for the next
// broadcast.
func (p *HTTPPool) observePeerVersion(baseURL string, version uint64) {
	if version <= p.RingVersion() {
		return
	}
	if !atomic.CompareAndSwapInt32(&p.ringSyncing, 0, 1) {
		return
	}
	go func() {
		defer atomic.StoreInt32(&p.ringSyncing, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Errors are ignored: the next peer response carrying a
		// newer version will trigger another attempt.
		p.syncRingFrom(ctx, baseURL)
	}()
}

// syncRingFrom fetches the ring state from the peer at baseURL
// (a peer base URL including the base path) and installs it if its
// version is newer than the local one.
func (p *HTTPPool) syncRingFrom(ctx context.Context, baseURL string) error {
	req, err := http.NewRequest("GET", baseURL+ringStatePath, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	tr := http.DefaultTransport
	if p.Transport != nil {
		tr = p.Transport(ctx)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	var state ringState
	if err := json.NewDecoder(res.Body).Decode(&state); err != nil {
		return fmt.Errorf("decoding ring state: %v", err)
	}
	p.SetWithVersion(state.Version, state.Peers...)
	return nil
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	if r.URL.Path == p.opts.BasePath+ringStatePath {
		p.serveRingState(w, r)
		return
	}
	w.Header().Set(ringVersionHeader, strconv.FormatUint(p.RingVersion(), 10))
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if p.Context != nil {
		ctx = p.Context(r)
	} else {
		ctx = r.Context()
	}
	if r.URL.Query().Get("handoff") == "1" {
		// This request is a handoff fetch from the key's new
		// owner. Serve it locally without consulting the ring
		// again, so it cannot bounce between peers.
		ctx = withHandoff(ctx)
	}

	group.Stats.ServerRequests.Add(1)
	var value []byte
	err := group.Get(ctx, key, AllocatingByteSliceSink(&value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

type httpGetter struct {
	transport func(context.Context) http.RoundTripper
	baseURL   string
	pool      *HTTPPool // nil for getters not created by a pool
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	if isHandoff(ctx) {
		u += "?handoff=1"
	}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(ctx)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if h.pool != nil {
		if v, err := strconv.ParseUint(res.Header.Get(ringVersionHeader), 10, 64); err == nil {
			h.pool.observePeerVersion(h.baseURL, v)
		}
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}
