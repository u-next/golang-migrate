package spanner

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type DistributedLock string

var globalTTL = 15 * time.Second

func newDistributedLock() DistributedLock {
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return newDistributedLockWithID(hex.EncodeToString(id))
}

func newDistributedLockWithID(id string) DistributedLock {
	return DistributedLock(fmt.Sprintf("%s,%s", id, time.Now().Add(globalTTL).Format(time.RFC3339)))
}

func (dl DistributedLock) Expired() bool {
	_, ttl := dl.parse()
	return time.Now().After(ttl)
}

func (dl DistributedLock) Equal(other DistributedLock) bool {
	partsA := strings.Split(string(dl), ",")
	partsB := strings.Split(string(other), ",")

	if len(partsA) != 2 || len(partsB) != 2 {
		return false
	}

	return partsA[0] == partsB[0]
}

func (dl DistributedLock) WithNewTTL() DistributedLock {
	id, _ := dl.parse()
	return newDistributedLockWithID(id)
}

func (dl DistributedLock) parse() (string, time.Time) {
	parts := strings.Split(string(dl), ",")

	if len(parts) != 2 {
		return "", time.Time{}
	}

	ttl, err := time.Parse(time.RFC3339, parts[1])
	if err != nil {
		return "", time.Time{}
	}

	return parts[0], ttl
}
