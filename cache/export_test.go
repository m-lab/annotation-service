package cache

import (
	"time"
	"unsafe"
)

type Entry = entry

func NewEntry(loader func() (interface{}, error), free func(interface{})) Entry {
	return Entry{loader: loader, free: free, err: ErrObjectUnloaded, lastUsed: unsafe.Pointer(&time.Time{})}
}
