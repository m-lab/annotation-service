package wrapper

// The AnnWrapper struct controls concurrent operations on Annotator objects.
// It is designed for minimal contention on GetAnnotator(), and safe loading and unloading.
// TODO - pull this out to an internal package, since it is only used by the directory.

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/annotation-service/metrics"
)

var (
	// ErrAnnotatorLoading is returned (externally) when an annotator is being loaded.
	ErrAnnotatorLoading = errors.New("annotator is being loaded")
	// ErrNilEntry is returned when wrapper is empty, and eligible for loading.
	ErrNilEntry = errors.New("map entry is nil")

	// These are UNEXPECTED errors!!
	// ErrGoroutineNotOwner is returned when goroutine attempts to set annotator entry, but is not the owner.
	// NOTE: this may happen if a goroutine for some reason calls unload when another goroutine is loading.
	ErrGoroutineNotOwner = errors.New("goroutine does not own annotator slot")

	// ErrMapEntryAlreadySet is returned when goroutine attempts to set annotator, but entry is non-nil.
	// This should never happen.
	ErrMapEntryAlreadySet = errors.New("annotator already set")
)

type AnnWrapper struct {
	// The lock must be held when accessing ann or err fields.
	lock sync.RWMutex

	// Updated only while holding write lock.
	ann api.Annotator

	// err field is used to indicate the wrapper status.
	// nil error means that the annotator is populated and ready for use.
	// An empty wrapper will have a non-nil error, indicating whether a previous load failed,
	// or annotator is currently loading, or annotator is nil and wrapper is idle.
	err error

	// This field is accessed using atomics.
	// In an empty wrapper, this should point to time.Time{} zero value.
	lastUsed unsafe.Pointer // pointer to the lastUsed time.Time.
}

// UpdateLastUsed updates the last used time to the current time.
func (ae *AnnWrapper) UpdateLastUsed() {
	newTime := time.Now()
	atomic.StorePointer(&ae.lastUsed, unsafe.Pointer(&newTime))
}

// GetLastUsed returns the time that the annotator was last successfully fetched with GetAnnotator.
func (ae *AnnWrapper) GetLastUsed() time.Time {
	ptr := atomic.LoadPointer(&ae.lastUsed)
	if ptr == nil {
		log.Println("Error in getLastUsed for", ae)
		return time.Time{}
	}
	return *(*time.Time)(ptr)
}

// ReserveForLoad attempts to set the state to loading, indicated by the `err` field
// containing ErrAnnotatorLoading.
// Returns true IFF the reservation was obtained.
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

// SetAnnotator attempts to store `ann`, and update the error state.
// It may fail if the state has changed, e.g. because of an unload.
func (ae *AnnWrapper) SetAnnotator(ann api.Annotator, err error) error {
	ae.lock.Lock()
	defer ae.lock.Unlock()

	metrics.PendingLoads.Dec()

	if ae.err != ErrAnnotatorLoading {
		// This may happen if another thread has caused an unload, though
		// this should not happen in normal operation.
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
	metrics.DatasetCount.Inc()

	return nil
}

// GetAnnotator gets the current annotator, if there is one, and the error state.
func (ae *AnnWrapper) GetAnnotator() (ann api.Annotator, err error) {
	ae.UpdateLastUsed()

	ae.lock.RLock()
	defer ae.lock.RUnlock()

	return ae.ann, ae.err
}

// Unload unloads the annotator and resets the state to the empty state.
func (ae *AnnWrapper) Unload() {
	ae.lock.Lock()
	defer ae.lock.Unlock()

	// If another goroutine is loading, then do nothing.
	if ae.err == ErrAnnotatorLoading {
		return
	}

	if ae.ann != nil {
		ae.ann.Close()
	}

	ae.ann = nil
	ae.err = ErrNilEntry
	atomic.StorePointer(&ae.lastUsed, unsafe.Pointer(&time.Time{}))
}

// New creates and initializes a new AnnWrapper
func New() AnnWrapper {
	return AnnWrapper{err: ErrNilEntry, lastUsed: unsafe.Pointer(&time.Time{})}
}
