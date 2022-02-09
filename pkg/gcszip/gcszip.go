package gcszip

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/golang/glog"
	"github.com/mhandugan/gcs-parallel-unzip/pkg/cache"
)

const gcsChunkSize = 2 << 20

// ListZipFile lists zip archive contents from a GCS file. The results
// can be used for local parallel operationers or to set up
// distributed processing. For distributed processing, the returned
// ObjectAttrs.Generation is important to retain, to ensure operations
// proceed only on the originally listed GCS object.  client may be
// nil, in which case a default client will be
// instantiated. `generation` may be 0 if unknown.
func ListZipFile(ctx context.Context, client *storage.Client, bucketName, zipObjectName string, maxParallelism int, generation int64) ([]*zip.File, *storage.ObjectAttrs, error) {
	if client == nil {
		var err error
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create GCS client: %w", err)
		}
	}
	o := client.Bucket(bucketName).Object(zipObjectName)
	oa, err := o.Attrs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to read metadata for gs://%s/%s: %w", bucketName, zipObjectName, err)
	}
	cachedReader := cache.New(
		&gcsReaderAt{ctx, o.Generation(oa.Generation)},
		oa.Size,
		maxParallelism,
		gcsChunkSize)
	zr, err := zip.NewReader(cachedReader, oa.Size)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to read ZIP file metadata from gs://%s/%s: %w", bucketName, zipObjectName, err)
	}
	return zr.File, oa, nil
}

func OpenFromZip(ctx context.Context, client *storage.Client, oa *storage.ObjectAttrs, zf *zip.File) (io.ReadCloser, error) {
	if zf.CompressedSize64 > gcsChunkSize && (zf.Method == zip.Store || zf.Method == zip.Deflate) {
		offset, err := zf.DataOffset()
		if err != nil {
			return nil, fmt.Errorf("Unable to determine file offset in gs://%s/%s for %q: %w", oa.Bucket, oa.Name, zf.Name, err)
		}
		r, err := client.Bucket(oa.Bucket).Object(oa.Name).Generation(oa.Generation).NewRangeReader(ctx, offset, int64(zf.CompressedSize64))
		if err != nil {
			return nil, err
		}
		if zf.Method == zip.Store {
			return io.NopCloser(r), nil
		}
		if zf.Method == zip.Deflate {
			return flate.NewReader(r), nil
		}
	}
	return zf.Open()
}

func Reopen(ctx context.Context, client *storage.Client, bucket, zipObjectName, zipContainedFileName string, zipObjectGeneration int64) (io.ReadCloser, *zip.File, error) {
	if client == nil {
		var err error
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create GCS client: %w", err)
		}
	}
	zfs, oa, err := ListZipFile(ctx, client, bucket, zipObjectName, 1, zipObjectGeneration)
	if err != nil {
		return nil, nil, err
	}
	for _, f := range zfs {
		if f.Name == zipContainedFileName {
			r, err := OpenFromZip(ctx, client, oa, f)
			return r, f, err
		}
	}
	return nil, nil, errors.New("Unable to find %q in gs//:%s/%s")
}

// gcsReaderAt adapts a GCS ObjectHandle to work with the archive/zip
// library.
type gcsReaderAt struct {
	ctx context.Context
	o   *storage.ObjectHandle
}

// ReadAt implements io.ReaderAt. ReadAt is a little different from
// Read in that it will block for input until it can fill the given
// buffer `p`.
func (r *gcsReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	glog.V(1).Infof("GCS read of %d bytes from offset %d in %q", len(p), off, r.o.ObjectName())
	rr, err := r.o.NewRangeReader(r.ctx, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer rr.Close()
	return io.ReadFull(rr, p)
}
