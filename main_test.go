package main

import (
	"bytes"
	"io"
	"testing"
)

func TestHello(t *testing.T) {
	in := bytes.NewReader([]byte("0123456789abcdefghijklmnopqrstuvwxyz"))
	c := &bCache{
		fsize:  int64(in.Len()),
		ra:     in,
		stride: 8,
		max:    10,
	}
	x := c.getB(3, 10)
	t.Errorf("%d entries", len(x))
	for _, z := range x {
		<-z.ready
		xx := make([]byte, 10)
		p, err := z.data.ReadAt(xx, 0)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		t.Errorf("%d %d %d %d %q", z.offset, z.sz, c.fsize, p, string(xx[:p]))
	}
	xx := make([]byte, 11)
	if b, err := c.ReadAt(xx, 0); err != nil || b != 10 {
		t.Errorf("%q %d %s", string(xx), b, err)
	}
}
