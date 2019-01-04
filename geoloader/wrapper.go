package geoloader

// The AnnWrapper struct controls concurrent operations on Annotator objects.
// It is designed for minimal contention on GetAnnotator(), and safe loading and unloading.
// TODO - pull this out to an internal package, since it is only used by the directory.

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/metrics"
)

type AnnWrapper struct {
	lock sync.RWMutex // Should be held when reading or modifying the annotator pointer or err fields
	// The lock must be held when accessing either of these.
	ann api.Annotator // Updated only while holding write lock.
	err error         // Non-nil if annotator is unloaded or loading, OR there was a non-recoverable load error.

	// This field is accessed using atomics.
	// In an empty wrapper, this should point to time.Time{} zero value.
	lastUsed unsafe.Pointer // pointer to the lastUsed time.Time.
}

func (ae *AnnWrapper) UpdateLastUsed() {
	newTime := time.Now()
	atomic.StorePointer(&ae.lastUsed, unsafe.Pointer(&newTime))
}

func (ae *AnnWrapper) GetLastUsed() time.Time {
	ptr := atomic.LoadPointer(&ae.lastUsed)
	if ptr == nil {
		log.Println("Error in getLastUsed for", ae)
		return time.Time{}
	}
	return *(*time.Time)(ptr)
}

func (ae *AnnWrapper) Status() error {
	ae.lock.RLock()
	defer ae.lock.RUnlock()
	return ae.err
}

func (ae *AnnWrapper) ReserveForLoad() bool {
	ae.lock.Lock()
	defer ae.lock.Unlock()
	if ae.err == nil {
		return false
	}
	if ae.err == ErrAnnotatorLoading { // This is the public error
		return false
	}
	// This takes ownership of the slot
	ae.err = ErrAnnotatorLoading
	return true
}

func (ae *AnnWrapper) SetAnnotator(ann api.Annotator, err error) error {
	ae.lock.Lock()
	defer ae.lock.Unlock()

	if ae.err != ErrAnnotatorLoading {
		log.Println("This should never happen", ErrGoroutineNotOwner)
		return ErrGoroutineNotOwner
	}
	if ae.ann != nil {
		log.Println("This should never happen", ErrMapEntryAlreadySet)
		return ErrMapEntryAlreadySet
	}
	ae.ann = ann
	ae.err = err
	ae.UpdateLastUsed()

	metrics.LoadCount.Inc()
	metrics.PendingLoads.Dec()
	metrics.DatasetCount.Inc()

	return nil
}

func (ae *AnnWrapper) GetAnnotator() (ann api.Annotator, err error) {
	ae.lock.RLock()
	defer ae.lock.RUnlock()
	return ae.ann, ae.err
}

func (ae *AnnWrapper) Unload() {
	ae.lock.Lock()
	defer ae.lock.Unlock()
	if ae.err == ErrAnnotatorLoading {
		// Someone is concurrently trying to load this.
		// We really don't care - the other goroutine will just fail.
	}

	if ae.ann != nil {
		ae.ann.Unload()
	}

	ae.ann = nil
	ae.err = ErrNilEntry
	atomic.StorePointer(&ae.lastUsed, unsafe.Pointer(&time.Time{}))
}

func NewAnnWrapper() AnnWrapper {
	return AnnWrapper{err: ErrNilEntry, lastUsed: unsafe.Pointer(&time.Time{})}
}
