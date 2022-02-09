package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"reflect"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/golang/glog"
	"github.com/mhandugan/gcs-parallel-unzip/pkg/gcszip"
)

var (
	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	input = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "", "Output file (required).")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*lsZipFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*zipLinesFn)(nil)))
	beam.RegisterType(reflect.TypeOf((*zipEmbeddedFile)(nil)))
	beam.RegisterType(reflect.TypeOf((*writeOutFileFn)(nil)))
}

var (
	wordRE          = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty           = beam.NewCounter("extract", "emptyLines")
	smallWordLength = flag.Int("small_word_length", 9, "length of small words (default: 9)")
	smallWords      = beam.NewCounter("extract", "smallWords")
	lineLen         = beam.NewDistribution("extract", "lineLenDistro")
)

type lsZipFn struct{}

func (l *lsZipFn) ProcessElement(ctx context.Context, zipObjectName string, emit func(zipEmbeddedFile)) error {
	parts := strings.SplitN(strings.TrimPrefix(zipObjectName, "gs://"), "/", 2)
	if len(parts) != 2 {
		return errors.New("missing a source zip file name")
	}
	bucket, object := parts[0], parts[1]

	fs, oa, err := gcszip.ListZipFile(ctx, nil, bucket, object, 1, 0)
	if err != nil {
		return err
	}
	for _, f := range fs {
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
		emit(zipEmbeddedFile{
			Bucket:     bucket,
			Object:     object,
			Generation: oa.Generation,
			Filename:   fn,
		})
	}
	return nil
}

type writeOutFileFn struct {
	Bucket string `json:"bucket"`
}

func (x *writeOutFileFn) ProcessElement(ctx context.Context, input zipEmbeddedFile, emit func(int64)) error {
	r, f, err := gcszip.Reopen(ctx, nil, input.Bucket, input.Object, input.Filename, input.Generation)
	if err != nil {
		return err
	}
	defer r.Close()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	w := client.Bucket(x.Bucket).Object(input.Filename).NewWriter(ctx)
	defer w.Close()
	w.Size = int64(f.UncompressedSize64)
	n, err := io.Copy(w, r)
	if err != nil {
		return err
	}
	emit(n)
	return nil
}

type zipEmbeddedFile struct {
	Bucket, Object, Filename string
	Generation               int64
}

type zipLinesFn struct{}

func (x *zipLinesFn) ProcessElement(ctx context.Context, input zipEmbeddedFile, emit func(string)) error {
	r, _, err := gcszip.Reopen(ctx, nil, input.Bucket, input.Object, input.Filename, input.Generation)
	if err != nil {
		return err
	}
	defer r.Close()
	reader := bufio.NewReader(r)
	for {
		s, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		emit(s)
	}
	return nil
}

func main() {
	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	fun := beam.ParDo(s, &lsZipFn{}, beam.Create(s, *input))
	//lines := beam.ParDo(s, &zipLinesFn{}, fun)

	outbkt := strings.SplitN(strings.TrimPrefix(*output, "gs://"), "/", 2)[0]

	beam.ParDo(s, &writeOutFileFn{Bucket: outbkt}, fun)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
