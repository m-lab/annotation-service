package cache

// The AnnWrapper struct controls concurrent operations on Annotator objects.
// It is designed for minimal contention on GetAnnotator(), and safe loading and unloading.
// TODO - pull this out to an internal package, since it is only used by the directory.

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/m-lab/annotation-service/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

/**************************************************************************************************
*                                            Errors                                               *
**************************************************************************************************/

// Error is the error type for all errors related to the  cache.
type Error interface {
	error
}

func newCacheError(msg string) Error {
	return Error(errors.New(msg))
}

var (
	// These errors are used in entry.err to indicate the entry's object status.  They may also
	// be returned by Cache.Get()

	// ErrObjectUnloaded is stored in entry.err when the object is nil, and no-one is loading it.
	ErrObjectUnloaded = newCacheError("object is not loaded")

	// ErrObjectLoading is returned when a new annotator is requested, but not yet loaded.
	ErrObjectLoading = newCacheError("object is loading")

	// ErrObjectLoadFailed is returned when a requested annotator has failed to load.
	ErrObjectLoadFailed = newCacheError("object load failed")

	// These are errors returned by the cache, but not stored in entries.

	// ErrTooManyLoading is returned when a new annotator is requested, but not yet loaded.
	ErrTooManyLoading = newCacheError("too many object already loading")

	// ErrCacheFull is returned when a new annotator is requested, but cache is full.
	ErrCacheFull = newCacheError("cache is full")

	// These are UNEXPECTED errors!!

	// ErrGoroutineNotOwner is returned when goroutine attempts to set object entry, but is not the owner.
	ErrGoroutineNotOwner = newCacheError("goroutine not owner")
	// ErrMapEntryAlreadySet is returned when goroutine attempts to set object, but entry is non-null.
	ErrMapEntryAlreadySet = newCacheError("map entry already set")
)

/**************************************************************************************************
*                                            Metrics                                              *
**************************************************************************************************/
var (
	ErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_error_total",
		Help: "The total number of cache errors.",
	}, []string{"type"})
)

func init() {
	prometheus.MustRegister(ErrorTotal)
}

func errorMetricWithLabel(err error) bool {
	if err != nil {
		_, _, line, _ := runtime.Caller(1)
		label := fmt.Sprintf("%3d %s", line+2, err)
		metrics.ErrorTotal.WithLabelValues(label).Inc()
		return true
	}
	return false
}

type entry struct {
	loader func() (interface{}, error) // functimmutable
	free   func(interface{})           // immutable

	// The lock must be held when accessing object or err fields.
	lock sync.RWMutex

	// Updated only while holding write lock.
	object interface{}

	// err field is used to indicate the entry status.
	// nil error means that the object is populated and ready for use.
	// An empty entry will have a non-nil error, indicating whether a previous load failed,
	// or object is currently loading, or object is nil and entry is idle.
	err     Error
	loadErr error // Optional additional error detail from a loading error.

	// This field is accessed using atomics.
	// In an empty entry, this should point to time.Time{} zero value.
	lastUsed unsafe.Pointer // pointer to the lastUsed time.Time.
}

// updateLastUsed updates the last used time to the current time.
func (ae *entry) UpdateLastUsed() {
	newTime := time.Now()
	atomic.StorePointer(&ae.lastUsed, unsafe.Pointer(&newTime))
}

// getLastUsed returns the time that the object was last successfully fetched with GetAnnotator.
func (ae *entry) GetLastUsed() time.Time {
	ptr := atomic.LoadPointer(&ae.lastUsed)
	if ptr == nil {
		log.Println("Error in getLastUsed for", ae)
		return time.Time{}
	}
	return *(*time.Time)(ptr)
}

// reserveForLoad attempts to set the state to loading, indicated by the `err` field
// containing ErrAnnotatorLoading.
// Returns true IFF the reservation was obtained.
func (ae *entry) ReserveForLoad() bool {
	ae.lock.Lock()
	defer ae.lock.Unlock()
	if ae.err == nil {
		return false
	}
	if ae.err == ErrObjectLoading { // This is the public error
		return false
	}
	// This takes ownership of the slot
	ae.err = ErrObjectLoading
	return true
}

// store attempts to store `object`, and update the error state.
func (ae *entry) Set(obj interface{}, err error) error {
	ae.lock.Lock()
	defer ae.lock.Unlock()

	metrics.PendingLoads.Dec()

	if ae.err != ErrObjectLoading {
		// This may happen if another thread has caused an unload, though
		// this should not happen in normal operation.
		return ErrGoroutineNotOwner
	}
	if ae.object != nil {
		log.Println("This should never happen", ErrMapEntryAlreadySet)
		return ErrMapEntryAlreadySet
	}
	ae.object = obj
	ae.err = err
	ae.UpdateLastUsed()

	metrics.LoadCount.Inc()
	metrics.DatasetCount.Inc()

	return nil
}

// GetAnnotator gets the current annotator, if there is one, and the error state.
func (ae *entry) Get() (interface{}, error) {
	ae.lock.RLock()
	defer ae.lock.RUnlock()
	ae.UpdateLastUsed()

	return ae.object, ae.err
}

// Unload unloads the annotator and resets the state to the empty state.
// If there was a previous load error, this clears it.
func (ae *entry) Unload() error {
	ae.lock.Lock()
	defer ae.lock.Unlock()

	// If another goroutine is loading, then do nothing.
	if ae.err == ErrObjectLoading {
		return ae.err
	}

	if ae.object != nil {
		ae.free(ae.object)
	}

	ae.object = nil
	ae.err = ErrObjectUnloaded
	atomic.StorePointer(&ae.lastUsed, unsafe.Pointer(&time.Time{}))

	return nil
}

// New creates and initializes a new entry.
func newEntry(loader func() (interface{}, error), free func(interface{})) entry {
	return entry{loader: loader, free: free, err: ErrObjectUnloaded, lastUsed: unsafe.Pointer(&time.Time{})}
}
