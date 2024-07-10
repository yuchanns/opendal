package opendal

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) List(path string) (*Lister, error) {
	list := getFFI[operatorList](op.ctx, symOperatorList)
	inner, err := list(op.inner, path)
	if err != nil {
		return nil, err
	}
	lister := &Lister{
		inner: inner,
		op:    op,
	}
	runtime.SetFinalizer(lister, func(_ *Lister) {
		free := getFFI[listerFree](op.ctx, symListerFree)
		free(inner)
	})
	return lister, nil
}

type Lister struct {
	inner *opendalLister
	op    *Operator // hold the op pointer to ensure it is gc after Lister instance.
	entry *Entry
}

func (l *Lister) Next() bool {
	next := getFFI[listerNext](l.op.ctx, symListerNext)
	inner, err := next(l.inner)
	if inner == nil && err == nil {
		l.entry = nil
		return false
	}

	entry := &Entry{
		op:    l.op,
		inner: inner,
		err:   err,
	}

	runtime.SetFinalizer(entry, func(_ *Entry) {
		free := getFFI[entryFree](l.op.ctx, symEntryFree)
		free(inner)
	})

	l.entry = entry
	return true
}

func (l *Lister) Entry() *Entry {
	return l.entry
}

type Entry struct {
	op    *Operator // hold the op pointer to ensure it is gc after Entry instance.
	inner *opendalEntry
	err   error
}

func (e *Entry) Error() error {
	return e.err
}

func (e *Entry) Name() string {
	name := getFFI[entryName](e.op.ctx, symEntryName)
	return name(e.inner)
}

func (e *Entry) Path() string {
	path := getFFI[entryPath](e.op.ctx, symEntryPath)
	return path(e.inner)
}

const symOperatorList = "opendal_operator_list"

type operatorList func(op *opendalOperator, path string) (*opendalLister, error)

var withOperatorList = withFFI(ffiOpts{
	sym:    symOperatorList,
	rType:  &typeResultList,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorList {
	return func(op *opendalOperator, path string) (*opendalLister, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result opendalResultList
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.err != nil {
			return nil, parseError(ctx, result.err)
		}
		return result.lister, nil
	}
})

const symListerFree = "opendal_lister_free"

type listerFree func(l *opendalLister)

var withListerFree = withFFI(ffiOpts{
	sym:    symListerFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) listerFree {
	return func(l *opendalLister) {
		ffiCall(
			nil,
			unsafe.Pointer(&l),
		)
	}
})

const symListerNext = "opendal_lister_next"

type listerNext func(l *opendalLister) (*opendalEntry, error)

var withListerNext = withFFI(ffiOpts{
	sym:    symListerNext,
	rType:  &typeResultListerNext,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) listerNext {
	return func(l *opendalLister) (*opendalEntry, error) {
		var result opendalResultListerNext
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&l),
		)
		if result.err != nil {
			return nil, parseError(ctx, result.err)
		}
		return result.entry, nil
	}
})

const symEntryFree = "opendal_entry_free"

type entryFree func(e *opendalEntry)

var withEntryFree = withFFI(ffiOpts{
	sym:    symEntryFree,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) entryFree {
	return func(e *opendalEntry) {
		ffiCall(
			nil,
			unsafe.Pointer(&e),
		)
	}
})

const symEntryName = "opendal_entry_name"

type entryName func(e *opendalEntry) string

var withEntryName = withFFI(ffiOpts{
	sym:    symEntryName,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) entryName {
	return func(e *opendalEntry) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&e),
		)
		return unix.BytePtrToString(bytePtr)
	}
})

const symEntryPath = "opendal_entry_path"

type entryPath func(e *opendalEntry) string

var withEntryPath = withFFI(ffiOpts{
	sym:    symEntryPath,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) entryPath {
	return func(e *opendalEntry) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&e),
		)
		return unix.BytePtrToString(bytePtr)
	}
})
