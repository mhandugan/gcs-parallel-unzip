package cache

import (
	"bytes"
	"io"
	"sort"
	"sync"

	"github.com/golang/glog"
)

// New returns a io.ReaderAt that provides very simple caching around
// the given ReaderAt. The fileSize must match the underlying file to
// avoid unexpected EOF errors during reads. The given blockSize
// should be a power of 2.
func New(reader io.ReaderAt, fileSize int64, maxBlocks, blockSize int) io.ReaderAt {
	return &blockCache{
		fsize:  fileSize,
		ra:     reader,
		stride: int64(blockSize),
		max:    maxBlocks,
	}
}

// blockCache implements a trivial read-through cache. This helps turn
// small (e.g. 30-byte) reads by archive/zip into larger reads to
// GCS. GCS charges per-op and has a per-request latency overhead. It
// is better to read larger chunks and re-use already-ready data as
// much as possible.
//
// Note: This is almost certainly where bugs will hide. Plenty of
// off-by-one opportunities here.
type blockCache struct {
	// mu protects bs
	mu sync.RWMutex
	// bs is sorted by offset of cacheEntry
	bs []*cacheEntry

	ra    io.ReaderAt
	fsize int64

	// Ideal size of each GCS read (currently also alignment of GCS
	// read). Must be a power of 2.
	stride int64

	// The max number of cacheEntry's to retain. Entries are freed in
	// the order they are added (not LRU).
	max int
	// lra tracks the entries in order they were added and will be
	// removed from the cache.
	lra []*cacheEntry
}

type cacheEntry struct {
	offset int64
	// size will be `stride` except for the final block of the gcs
	// object.
	sz   int
	data *bytes.Reader
	err  error
	// An underlying read starts async when the entry is added to the
	// cache. Before using `data` or `err`, block reading from ths
	// chan.
	ready <-chan bool
}

// ReadAt implements io.ReaderAt for the read-through cache.
func (c *blockCache) ReadAt(p []byte, off int64) (n int, err error) {
	glog.V(1).Infof("Read-through cache read of %d bytes at offset %d", len(p), off)
	xs := c.getB(off, int64(len(p)))
	n = 0
	for _, x := range xs {
		<-x.ready
		if x.err != nil {
			return n, x.err
		}
		// doff is the offset in x.data
		doff := int(off - x.offset)
		l := x.sz - doff
		if n+l > len(p) {
			l = len(p) - n
		}
		n2, err := x.data.ReadAt(p[n:n+l], int64(doff))
		n += n2
		off += int64(n2)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// tryGetB does not perform locking. Attempts to find already-cached
// block that spans the given offset.
func (c *blockCache) tryGetB(off int64) (ret *cacheEntry) {
	i := 0
	if len(c.bs) != 0 {
		i = sort.Search(len(c.bs), func(i int) bool { return c.bs[i].offset+int64(c.bs[i].sz)-1 >= off })
	}
	if i >= len(c.bs) || c.bs[i].offset > off {
		return nil
	}
	return c.bs[i]
}

// getB returns blocks spanning the given range, in order of start
// offset. Creates new blocks where needed.
func (c *blockCache) getB(off, sz int64) (ret []*cacheEntry) {
	for sz > 0 {
		c.mu.RLock()
		buf := c.tryGetB(off)
		c.mu.RUnlock()
		if buf == nil {
			l := c.stride
			boff := off & ^(c.stride - 1)
			wtf := (off & (c.stride - 1))
			off = boff
			sz += wtf
			if boff+l >= c.fsize {
				l = c.fsize - boff
			}
			glog.V(1).Infof("%% %d %d %d %d %d", off, boff, sz, l, wtf)
			ret = append(ret, c.putB(boff, int(l)))
		} else {
			ret = append(ret, buf)
		}
		off += c.stride
		sz -= c.stride
	}

	return ret
}

// putB inserts a new block for the given offset and size, unless one
// exists in the cache.
func (c *blockCache) putB(off int64, sz int) *cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if buf := c.tryGetB(off); buf != nil {
		// TODO: should verify the buf is an exact match. If we ever
		// relax the alignment constraint, overlapping blocks might be
		// a thing.
		return buf
	}

	d := make([]byte, sz)
	fin := make(chan bool, 0)
	e := &cacheEntry{
		offset: off,
		sz:     sz,
		ready:  fin,
	}
	go func() {
		e.sz, e.err = c.ra.ReadAt(d, off)
		e.data = bytes.NewReader(d)
		close(fin)
	}()
	c.bs = append(c.bs, e)
	glog.V(1).Infof("adding cache entry for offset %d", e.offset)

	sort.Sort(c)
	c.lra = append(c.lra, e)
	over := len(c.lra) - c.max
	if over > 0 {
		var tokill []*cacheEntry
		tokill, c.lra = c.lra[0:over], c.lra[over:]
		for _, tk := range tokill {
			i := sort.Search(len(c.bs), func(i int) bool { return c.bs[i].offset+int64(c.bs[i].sz)-1 >= tk.offset })
			c.bs = append(c.bs[:i], c.bs[i+1:]...)
			glog.V(1).Infof("deleting cache block for offset %d", tk.offset)
		}
	}
	return e
}

func (c *blockCache) Len() int {
	return len(c.bs)
}

func (c *blockCache) Less(i, j int) bool {
	return c.bs[i].offset < c.bs[j].offset
}

func (c *blockCache) Swap(i, j int) {
	c.bs[i], c.bs[j] = c.bs[j], c.bs[i]
}
