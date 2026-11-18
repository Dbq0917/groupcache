package groupcache

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/golang/protobuf/proto"

	pb "github.com/golang/groupcache/groupcachepb"
)

func TestNotFoundSemanticsLocal(t *testing.T) {
	tests := []struct {
		name       string
		getter     GetterFunc
		wantErr    bool
		wantValue  string
		wantFills  int32
		wantCached int64
	}{
		{
			name: "sentinel",
			getter: func(context.Context, string, Sink) error {
				return &ErrNotFound{Msg: "missing"}
			},
			wantErr:    true,
			wantFills:  2,
			wantCached: 0,
		},
		{
			name: "wrapped sentinel",
			getter: func(context.Context, string, Sink) error {
				return fmt.Errorf("source: %w", &ErrNotFound{Msg: "missing"})
			},
			wantErr:    true,
			wantFills:  2,
			wantCached: 0,
		},
		{
			name: "no value set",
			getter: func(context.Context, string, Sink) error {
				return nil
			},
			wantErr:    true,
			wantFills:  2,
			wantCached: 0,
		},
		{
			name: "empty value",
			getter: func(_ context.Context, _ string, dest Sink) error {
				return dest.SetString("")
			},
			wantValue:  "",
			wantFills:  1,
			wantCached: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fills int32
			getter := GetterFunc(func(ctx context.Context, key string, dest Sink) error {
				fills++
				return tt.getter(ctx, key, dest)
			})
			g := newGroup(t.Name(), 1<<20, getter, NoPeers{})

			for i := 0; i < 2; i++ {
				var got string
				err := g.Get(context.Background(), "key", StringSink(&got))
				if tt.wantErr {
					if !errors.Is(err, &ErrNotFound{}) {
						t.Fatalf("Get #%d error = %v; want ErrNotFound", i+1, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("Get #%d error = %v; want nil", i+1, err)
				}
				if got != tt.wantValue {
					t.Errorf("Get #%d value = %q; want %q", i+1, got, tt.wantValue)
				}
			}

			if fills != tt.wantFills {
				t.Errorf("getter calls = %d; want %d", fills, tt.wantFills)
			}
			if got := g.mainCache.items(); got != tt.wantCached {
				t.Errorf("cached items = %d; want %d", got, tt.wantCached)
			}
		})
	}
}

func TestReusedSinkDoesNotTreatOldValueAsNew(t *testing.T) {
	var fills int32
	g := newGroup(t.Name(), 1<<20, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		fills++
		if key == "missing" {
			return nil
		}
		return dest.SetString("value")
	}), NoPeers{})

	var got string
	sink := StringSink(&got)
	if err := g.Get(context.Background(), "present", sink); err != nil {
		t.Fatal(err)
	}
	if got != "value" {
		t.Fatalf("first Get = %q; want %q", got, "value")
	}
	if err := g.Get(context.Background(), "missing", sink); !errors.Is(err, &ErrNotFound{}) {
		t.Fatalf("second Get error = %v; want ErrNotFound", err)
	}
	if fills != 2 {
		t.Errorf("getter calls = %d; want 2", fills)
	}
}

type errorPeer struct {
	err   error
	calls int
}

func (p *errorPeer) Get(context.Context, *pb.GetRequest, *pb.GetResponse) error {
	p.calls++
	return p.err
}

type singlePeer struct {
	peer ProtoGetter
}

func (p singlePeer) PickPeer(string) (ProtoGetter, bool) {
	return p.peer, true
}

type valuePeer struct {
	value []byte
}

func (p *valuePeer) Get(_ context.Context, _ *pb.GetRequest, out *pb.GetResponse) error {
	out.Value = p.value
	return nil
}

func TestPeerNotFoundIsTerminal(t *testing.T) {
	for _, peerErr := range []error{
		&ErrNotFound{Msg: "missing"},
		fmt.Errorf("owner: %w", &ErrNotFound{Msg: "missing"}),
	} {
		t.Run(fmt.Sprintf("%T", peerErr), func(t *testing.T) {
			peer := &errorPeer{err: peerErr}
			var localCalls int
			g := newGroup(t.Name(), 1<<20, GetterFunc(func(_ context.Context, _ string, dest Sink) error {
				localCalls++
				return dest.SetString("local")
			}), singlePeer{peer: peer})

			var got string
			if err := g.Get(context.Background(), "key", StringSink(&got)); !errors.Is(err, &ErrNotFound{}) {
				t.Fatalf("Get error = %v; want ErrNotFound", err)
			}
			if localCalls != 0 {
				t.Errorf("local getter calls = %d; want 0", localCalls)
			}
			if peer.calls != 1 {
				t.Errorf("peer calls = %d; want 1", peer.calls)
			}
			if got := g.mainCache.items(); got != 0 {
				t.Errorf("cached items = %d; want 0", got)
			}
		})
	}
}

func TestPeerEmptyValueSemantics(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		wantErr bool
	}{
		{name: "omitted value", value: nil, wantErr: true},
		{name: "present empty value", value: []byte{}, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var localCalls int
			g := newGroup(t.Name(), 1<<20, GetterFunc(func(_ context.Context, _ string, dest Sink) error {
				localCalls++
				return dest.SetString("local")
			}), singlePeer{peer: &valuePeer{value: tt.value}})

			var got string
			err := g.Get(context.Background(), "key", StringSink(&got))
			if tt.wantErr {
				if !errors.Is(err, &ErrNotFound{}) {
					t.Fatalf("Get error = %v; want ErrNotFound", err)
				}
			} else if err != nil {
				t.Fatalf("Get error = %v; want nil", err)
			}
			if localCalls != 0 {
				t.Errorf("local getter calls = %d; want 0", localCalls)
			}
			if got != "" {
				t.Errorf("Get value = %q; want empty", got)
			}
		})
	}
}

func TestHTTPNotFoundSemantics(t *testing.T) {
	const groupName = "http-not-found-semantics"
	getter := GetterFunc(func(_ context.Context, key string, dest Sink) error {
		if key == "empty" {
			return dest.SetString("")
		}
		return &ErrNotFound{Msg: "missing from source"}
	})
	newGroup(groupName, 1<<20, getter, NoPeers{})

	const basePath = "/_groupcache/"
	pool := &HTTPPool{opts: HTTPPoolOptions{BasePath: basePath}}
	server := httptest.NewServer(pool)
	defer server.Close()

	// The serving side maps ErrNotFound to 404.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", basePath+groupName+"/missing", nil)
	pool.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Errorf("ServeHTTP status = %d; want 404", rec.Code)
	}

	// The client side maps 404 back to ErrNotFound.
	hg := &httpGetter{baseURL: server.URL + basePath}
	var res pb.GetResponse
	err := hg.Get(context.Background(), &pb.GetRequest{
		Group: proto.String(groupName),
		Key:   proto.String("missing"),
	}, &res)
	if !errors.Is(err, &ErrNotFound{}) {
		t.Fatalf("httpGetter error = %v; want ErrNotFound", err)
	}

	// An empty value is a successful hit, not a miss.
	err = hg.Get(context.Background(), &pb.GetRequest{
		Group: proto.String(groupName),
		Key:   proto.String("empty"),
	}, &res)
	if err != nil {
		t.Fatalf("httpGetter empty value error = %v; want nil", err)
	}
	if res.Value == nil {
		t.Error("empty value field is missing; want a present empty value")
	}
	if len(res.Value) != 0 {
		t.Errorf("empty value = %q; want empty", res.Value)
	}
}
