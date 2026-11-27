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
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/groupcache/consistenthash"
	"github.com/golang/protobuf/proto"

	pb "github.com/golang/groupcache/groupcachepb"
)

var (
	peerAddrs = flag.String("test_peer_addrs", "", "Comma-separated list of peer addresses; used by TestHTTPPool")
	peerIndex = flag.Int("test_peer_index", -1, "Index of which peer this child is; used by TestHTTPPool")
	peerChild = flag.Bool("test_peer_child", false, "True if running as a child process; used by TestHTTPPool")
)

func TestHTTPPool(t *testing.T) {
	if *peerChild {
		beChildForTestHTTPPool()
		os.Exit(0)
	}

	const (
		nChild = 4
		nGets  = 100
	)

	var childAddr []string
	for i := 0; i < nChild; i++ {
		childAddr = append(childAddr, pickFreeAddr(t))
	}

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nChild; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestHTTPPool",
			"--test_peer_child",
			"--test_peer_addrs="+strings.Join(childAddr, ","),
			"--test_peer_index="+strconv.Itoa(i),
		)
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, childAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nChild; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	// Use a dummy self address so that we don't handle gets in-process.
	p := NewHTTPPool("should-be-ignored")
	p.Set(addrToURL(childAddr)...)

	// Dummy getter function. Gets should go to children only.
	// The only time this process will handle a get is when the
	// children can't be contacted for some reason.
	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		return errors.New("parent getter called; something's wrong")
	})
	g := NewGroup("httpPoolTest", 1<<20, getter)

	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(context.TODO(), key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

// TestHTTPPoolDelete verifies the HTTP DELETE wire path invalidates the
// key on the serving peer and that the following GET reloads it.
func TestHTTPPoolDelete(t *testing.T) {
	// Build the pool struct directly: NewHTTPPoolOpts can only run once
	// per process and TestHTTPPool already claims it.
	p := &HTTPPool{
		self:        "http://delete-test",
		httpGetters: make(map[string]*httpGetter),
		opts: HTTPPoolOptions{
			BasePath: defaultBasePath,
			Replicas: defaultReplicas,
		},
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	var fills AtomicInt
	g := newGroup(uniqueGroupName(t), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		fills.Add(1)
		return dest.SetString("ECHO:" + key)
	}), p)

	srv := httptest.NewServer(p)
	defer srv.Close()
	p.self = srv.URL

	getter := &httpGetter{baseURL: srv.URL + defaultBasePath}
	req := &pb.GetRequest{
		Group: proto.String(g.name),
		Key:   proto.String("mykey"),
	}

	res := &pb.GetResponse{}
	if err := getter.Get(context.TODO(), req, res); err != nil {
		t.Fatal(err)
	}
	if string(res.GetValue()) != "ECHO:mykey" {
		t.Fatalf("got %q", res.GetValue())
	}
	if fills.Get() != 1 {
		t.Fatalf("fills = %d, want 1", fills.Get())
	}

	// A second GET is served from the cache.
	if err := getter.Get(context.TODO(), req, &pb.GetResponse{}); err != nil {
		t.Fatal(err)
	}
	if fills.Get() != 1 {
		t.Fatalf("fills = %d, want 1 (cached)", fills.Get())
	}

	if err := getter.Delete(context.TODO(), req, &pb.GetResponse{}); err != nil {
		t.Fatal(err)
	}

	if err := getter.Get(context.TODO(), req, &pb.GetResponse{}); err != nil {
		t.Fatal(err)
	}
	if fills.Get() != 2 {
		t.Errorf("fills = %d after Delete, want 2", fills.Get())
	}

	// Unsupported methods are rejected explicitly.
	httpReq, err := http.NewRequest("PATCH",
		srv.URL+defaultBasePath+g.name+"/mykey", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultTransport.RoundTrip(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("PATCH status = %v, want %v", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}

func beChildForTestHTTPPool() {
	addrs := strings.Split(*peerAddrs, ",")

	p := NewHTTPPool("http://" + addrs[*peerIndex])
	p.Set(addrToURL(addrs)...)

	getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
		dest.SetString(strconv.Itoa(*peerIndex) + ":" + key)
		return nil
	})
	NewGroup("httpPoolTest", 1<<20, getter)

	log.Fatal(http.ListenAndServe(addrs[*peerIndex], p))
}

// This is racy. Another process could swoop in and steal the port between the
// call to this function and the next listen call. Should be okay though.
// The proper way would be to pass the l.File() as ExtraFiles to the child
// process, and then close your copy once the child starts.
func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

func awaitAddrReady(t *testing.T, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	const max = 1 * time.Second
	tries := 0
	for {
		tries++
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		delay := time.Duration(tries) * 25 * time.Millisecond
		if delay > max {
			delay = max
		}
		time.Sleep(delay)
	}
}
