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
	"time"

	pb "github.com/golang/groupcache/groupcachepb"
)

const (
	// frameRetryCount is the number of attempts made for a single
	// frame before the whole Get fails. A retry re-requests only the
	// failed byte range, so a flaky link no longer restarts a large
	// object transfer from zero.
	frameRetryCount = 3

	frameRetryDelay = 50 * time.Millisecond
)

// maxFrameBytes bounds the payload carried in a single peer
// response frame. It must stay below the transport's per-message
// size limit together with the protobuf frame overhead; peers stream
// larger objects in consecutive, independently retried frames. It is
// a variable rather than a constant so deployments with a different
// transport ceiling can tune it before issuing Gets.
var maxFrameBytes int64 = 64 << 10 // 64 KiB

// rangePeerGetter is implemented by peers that can serve a byte
// range of a value in bounded-size frames. Peers that do not
// implement it fall back to a single whole-object Get, preserving
// compatibility with mixed-version deployments.
type rangePeerGetter interface {
	GetRange(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error
}

// getFromPeerChunked fetches value from peer one frame at a time.
// Only one frame is resident beyond the single, exactly-sized
// result buffer, so peak extra memory on the receiving side is
// len(value)+maxFrameBytes rather than a multiple of len(value).
func (g *Group) getFromPeerChunked(ctx context.Context, peer ProtoGetter, key string) (ByteView, error) {
	ranger, ok := peer.(rangePeerGetter)
	if !ok {
		return g.getFromPeer(ctx, peer, key)
	}

	first, err := g.fetchFrame(ctx, ranger, key, 0, maxFrameBytes)
	if err != nil {
		return ByteView{}, err
	}

	// A size-bearing response means the peer understood the range
	// request and the object may span several frames. A legacy peer
	// ignores the range parameters, sets no size and returns the
	// whole object in one frame; trust it as-is so rolling upgrades
	// keep working.
	if first.Size == nil {
		return ByteView{b: first.Value}, nil
	}
	total := first.GetSize()
	if total < 0 {
		return ByteView{}, fmt.Errorf("groupcache: peer reported negative size %d", total)
	}
	if int64(len(first.Value)) > total {
		return ByteView{}, fmt.Errorf("groupcache: first frame of %d bytes exceeds declared size %d", len(first.Value), total)
	}

	// One allocation sized to the whole object. Frames are copied in
	// place and then become garbage; no second full-size copy exists.
	buf := make([]byte, 0, total)
	buf = append(buf, first.Value...)

	for offset := int64(len(buf)); offset < total; {
		limit := total - offset
		if limit > maxFrameBytes {
			limit = maxFrameBytes
		}
		frame, err := g.fetchFrame(ctx, ranger, key, offset, limit)
		if err != nil {
			return ByteView{}, err
		}
		if frame.GetSize() != total {
			return ByteView{}, fmt.Errorf("groupcache: peer size changed mid-fetch: %d then %d", total, frame.GetSize())
		}
		if len(frame.Value) == 0 {
			return ByteView{}, fmt.Errorf("groupcache: peer returned empty frame at offset %d", offset)
		}
		if int64(len(buf))+int64(len(frame.Value)) > total {
			return ByteView{}, fmt.Errorf("groupcache: peer returned more than %d bytes", total)
		}
		buf = append(buf, frame.Value...)
		offset += int64(len(frame.Value))
	}

	return ByteView{b: buf}, nil
}

// fetchFrame requests one byte range, retrying only that range on
// transient failures. Each attempt uses a fresh response message, so
// a failed frame's partial bytes are discarded rather than
// accumulated.
func (g *Group) fetchFrame(ctx context.Context, peer rangePeerGetter, key string, offset, limit int64) (*pb.GetResponse, error) {
	var lastErr error
	for attempt := 0; attempt < frameRetryCount; attempt++ {
		if attempt > 0 {
			if err := sleepWithContext(ctx, frameRetryDelay*time.Duration(attempt)); err != nil {
				return nil, err
			}
		}
		req := &pb.GetRequest{
			Group:  &g.name,
			Key:    &key,
			Offset: &offset,
			Limit:  &limit,
		}
		res := &pb.GetResponse{}
		err := peer.GetRange(ctx, req, res)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("groupcache: frame at offset %d failed after %d attempts: %w", offset, frameRetryCount, lastErr)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
