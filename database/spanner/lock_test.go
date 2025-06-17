package spanner

import (
	"testing"
	"time"

	"github.com/ysmood/got"
)

func TestDistributedLock(t *testing.T) {
	g := got.T(t)

	globalTTL = 3 * time.Second

	x := newDistributedLock()
	y := newDistributedLock()

	time.Sleep(1200 * time.Millisecond)
	z := x.WithNewTTL()

	g.Eq(x.Expired(), false)
	g.Len(x, 58)

	g.True(x.Equal(x))
	g.False(x.Equal(y))

	g.True(x.Equal(z))

	time.Sleep(3 * time.Second)
	g.Eq(x.Expired(), true)
}
