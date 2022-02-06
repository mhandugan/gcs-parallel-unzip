package main

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"flag"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

var (
	sourceBucket   = flag.String("source_bucket", "zip-source-1t", "source bucket name")
	zipFile        = flag.String("zip_file", "big.zip", "object id of zip file in the source bucket")
	destBucket     = flag.String("destination_bucket", "zip-dest-1t", "destination bucket name")
	numWorkers     = flag.Int("workers", 80, "number of workers")
	workQueueDepth = flag.Int("queue", 2000, "number of queued files")
	stride         = flag.Int("stride", 2<<20, "power of 2")
	showProgress   = flag.Bool("progress", false, "show progress bars")
)

type bEntry struct {
	offset int64
	sz     int
	data   *bytes.Reader
	err    error
	ready  <-chan bool
}

type bCache struct {
	mu     sync.RWMutex
	bs     []*bEntry
	ra     io.ReaderAt
	fsize  int64
	stride int64
	max    int
	lra    []*bEntry
}

func (c *bCache) ReadAt(p []byte, off int64) (n int, err error) {
	//glog.Printf("^^ %d %d", len(p), off)
	xs := c.getB(off, int64(len(p)))
	n = 0
	for _, x := range xs {
		<-x.ready
		if x.err != nil {
			return n, x.err
		}
		doff := int(off - x.offset)
		l := x.sz - doff

		if n+l > len(p) {
			l = len(p) - n
		}
		//		glog.Printf(">> x.off=%d x.sz=%d %d %d %d", x.offset, x.sz, n, l, doff)
		n2, err := x.data.ReadAt(p[n:n+l], int64(doff))
		n += n2
		off += int64(n2)
		if err != nil /* && err != io.EOF */ {
			return n, err
		}
	}
	return n, nil
}

func (c *bCache) tryGetB(off int64) (ret *bEntry) {
	i := 0
	if len(c.bs) != 0 {
		i = sort.Search(len(c.bs), func(i int) bool { return c.bs[i].offset+int64(c.bs[i].sz)-1 >= off })
	}
	if i >= len(c.bs) || c.bs[i].offset > off {
		return nil
	}
	return c.bs[i]
}

func (c *bCache) getB(off, sz int64) (ret []*bEntry) {
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
			//			glog.Printf("%% %d %d %d %d %d", off, boff, sz, l, wtf)
			ret = append(ret, c.putB(boff, int(l)))
		} else {
			ret = append(ret, buf)
		}
		off += c.stride
		sz -= c.stride
	}

	return ret
}

func (c *bCache) putB(off int64, sz int) *bEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if buf := c.tryGetB(off); buf != nil {
		return buf
	}

	d := make([]byte, sz)
	fin := make(chan bool, 0)
	e := &bEntry{
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
	//glog.Printf("adding %d", e.offset)

	sort.Sort(c)
	c.lra = append(c.lra, e)
	over := len(c.lra) - c.max
	if over > 0 {
		var tokill []*bEntry
		tokill, c.lra = c.lra[0:over], c.lra[over:]
		for _, tk := range tokill {
			i := sort.Search(len(c.bs), func(i int) bool { return c.bs[i].offset+int64(c.bs[i].sz)-1 >= tk.offset })
			c.bs = append(c.bs[:i], c.bs[i+1:]...)
			//glog.Printf("deleting %d", tk.offset)
		}
	}
	return e
}

func (c *bCache) Len() int {
	return len(c.bs)
}

func (c *bCache) Less(i, j int) bool {
	return c.bs[i].offset < c.bs[j].offset
}

func (c *bCache) Swap(i, j int) {
	c.bs[i], c.bs[j] = c.bs[j], c.bs[i]
}

type files struct {
	fs []*zip.File
}

func (c *files) Len() int {
	return len(c.fs)
}

func (c *files) Less(i, j int) bool {
	return c.fs[i].UncompressedSize64 < c.fs[j].UncompressedSize64
}

func (c *files) Swap(i, j int) {
	c.fs[i], c.fs[j] = c.fs[j], c.fs[i]
}

type rdr struct {
	ctx context.Context
	o   *storage.ObjectHandle
}

func (r *rdr) ReadAt(p []byte, off int64) (n int, err error) {
	//glog.Printf("reading %d bytes from %d", len(p), off)
	rr, err := r.o.NewRangeReader(r.ctx, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer rr.Close()
	return io.ReadFull(rr, p)
}

func init() {
}

func main() {
	flag.Parse()
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("oops! %s", err)
	}
	bkt := client.Bucket(*sourceBucket)
	o := bkt.Object(*zipFile)
	oa, err := o.Attrs(ctx)
	if err != nil {
		log.Fatal(err)
	}

	cachedReader := &bCache{
		fsize:  oa.Size,
		ra:     &rdr{ctx, o.Generation(oa.Generation)},
		stride: int64(*stride),
		max:    *numWorkers,
	}
	zr, err := zip.NewReader(cachedReader, oa.Size)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Infof("%s/%s has %d entries!", oa.Bucket, oa.Name, len(zr.File))
	fs := &files{}
	var totalOutputSize int64
	for _, f := range zr.File {
		fn := f.Name
		if strings.HasSuffix(fn, "/") {
			glog.V(1).Infof("skipping directory %q", fn)
			continue
		}
		if f.NonUTF8 {
			glog.V(1).Infof("skipping non-utf8 entry")
			continue
		}
		if strings.HasPrefix(fn, "/") {
			glog.V(1).Infof("skipping invalid entry")
			continue
		}
		fs.fs = append(fs.fs, f)
		totalOutputSize += int64(f.UncompressedSize64)
	}
	prog := mpb.NewWithContext(ctx,
		mpb.WithRefreshRate(time.Second),
		mpb.ContainerOptOn(mpb.WithOutput(nil),
			func() bool {
				return !*showProgress
			}),
	)
	bar := prog.AddBar(totalOutputSize,
		// mpb.BarRemoveOnComplete(),
		// mpb.PrependDecorators(decor.Name(fn)),
		mpb.PrependDecorators(decor.AverageSpeed(decor.UnitKB, "%.2f")),
	)

	if fs.Len() > 0 {
		sort.Sort(sort.Reverse(fs))
	}

	db := client.Bucket(*destBucket)
	var wg sync.WaitGroup
	workQueue := make(chan *zip.File, *workQueueDepth)
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			glog.V(1).Infof("worker %d says hello!", i)
			for f := range workQueue {
				func() {
					fn := f.Name
					var (
						err    error
						x      io.Reader
						closer io.Closer
					)
					if int64(f.CompressedSize64) > int64(*stride) {
						if f.Method == zip.Store {
							offset, err := f.DataOffset()
							if err != nil {
								log.Print(err)
								return
							}
							x, err = bkt.Object(*zipFile).NewRangeReader(ctx, offset, int64(f.UncompressedSize64))
							closer = x.(io.Closer)
							if err != nil {
								log.Print(err)
								return
							}
						} else if f.Method == zip.Deflate {
							offset, err := f.DataOffset()
							if err != nil {
								log.Print(err)
								return
							}
							raw, err := bkt.Object(*zipFile).NewRangeReader(ctx, offset, int64(f.CompressedSize64))
							if err != nil {
								log.Print(err)
								return
							}
							closer = raw
							x = flate.NewReader(raw)
						} else {
							glog.V(1).Infof("##### Skipping %s with size %dKB", f.Name, f.UncompressedSize64/1024)
							return
						}
					} else {
						x, err = f.Open()
						closer = x.(io.Closer)
						if err != nil {
							glog.V(1).Infof("shoot! %s", err)
							return
						}
					}
					defer closer.Close()
					// .If(storage.Conditions{DoesNotExist: true})
					w := db.Object(fn).NewWriter(ctx)
					w.Size = int64(f.UncompressedSize64)
					// bar := prog.AddBar(w.Size,
					// 	mpb.BarRemoveOnComplete(),
					// 	mpb.PrependDecorators(decor.Name(fn)),
					// 	mpb.AppendDecorators(decor.AverageSpeed(decor.UnitKB, "%.2f")),
					// )
					w.ProgressFunc = func(c int64) { bar.SetCurrent(c) }
					// w.CRC32C = f.CRC32 nope!
					if _, err := io.Copy(w, x); err != nil {
						glog.V(1).Infof("rats %s", err)
						// bar.Abort(false)
						return
					}
					if err = w.Close(); err != nil {
						glog.V(1).Infof("boom %s failed with %s", f.Name, err)
						// bar.Abort(false)
						return
					}
					glog.V(1).Infof(" worker %d wrote %s!", i, f.Name)
				}()
			}
			glog.V(1).Infof("Worker %d says bye!", i)
		}(i)
	}

	for _, f := range fs.fs {
		workQueue <- f
	}
	close(workQueue)
	wg.Wait()
	//prog.Wait()
}
