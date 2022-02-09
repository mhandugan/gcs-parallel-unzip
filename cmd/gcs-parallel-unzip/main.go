package main

import (
	"archive/zip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"

	"github.com/mhandugan/gcs-parallel-unzip/pkg/gcszip"
)

var (
	sourceBucket   = flag.String("source_bucket", "zip-source-1t", "source bucket name")
	zipFile        = flag.String("zip_file", "big.zip", "object id of zip file in the source bucket")
	destBucket     = flag.String("destination_bucket", "zip-dest-1t", "destination bucket name")
	numWorkers     = flag.Int("workers", 10, "number of workers")
	workQueueDepth = flag.Int("queue", 2000, "number of queued files")
	showProgress   = flag.Bool("progress", false, "show progress bars")
)

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

func main() {
	flag.Parse()
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("unable to create GCS client: %s", err))
	}

	zfs, oa, err := gcszip.ListZipFile(ctx, nil, *sourceBucket, *zipFile, *numWorkers, 0)
	if err != nil {
		log.Fatal(err)
	}
	glog.Infof("%s/%s has %d entries!", oa.Bucket, oa.Name, len(zfs))

	fs := &files{}
	var totalOutputSize int64
	for _, f := range zfs {
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
		mpb.PrependDecorators(decor.AverageSpeed(decor.UnitKB, "%.2f")),
	)

	if fs.Len() > 0 {
		sort.Sort(sort.Reverse(fs))
	}
	var progress int64 = 0
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
					x, err := gcszip.OpenFromZip(ctx, client, oa, f)
					if err != nil {
						log.Fatal(err)
					}
					defer x.Close()

					w := db.Object(fn).NewWriter(ctx)
					w.Size = int64(f.UncompressedSize64)
					var localProgress int64 = 0
					w.ProgressFunc = func(c int64) { bar.SetCurrent(atomic.AddInt64(&progress, c-localProgress)); localProgress = c }
					if _, err := io.Copy(w, x); err != nil {
						glog.V(1).Infof("rats %s", err)
						return
					}
					if err = w.Close(); err != nil {
						glog.V(1).Infof("boom %s failed with %s", f.Name, err)
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
	bar.Abort(false)
	prog.Wait()
}
