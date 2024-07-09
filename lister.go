package opendal

import (
	"context"
	"errors"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) List(path string) (*Lister, error) {
	list := getCFunc[operatorList](o.ctx, symOperatorList)
	inner, err := list(o.inner, path)
	if err != nil {
		return nil, err
	}
	lister := &Lister{
		inner: inner,
		ctx:   o.ctx,
	}
	runtime.SetFinalizer(lister, func(_ *Lister) {
		free := getCFunc[listerFree](o.ctx, symListerFree)
		free(inner)
	})
	return lister, nil
}

type Lister struct {
	inner *opendalLister
	ctx   context.Context
	entry *Entry
}

func (l *Lister) Next() bool {
	next := getCFunc[listerNext](l.ctx, symListerNext)
	inner, err := next(l.inner)
	if inner == nil && err == nil {
		l.entry = nil
		return false
	}

	entry := &Entry{
		ctx:   l.ctx,
		inner: inner,
		err:   err,
	}

	runtime.SetFinalizer(entry, func(_ *Entry) {
		free := getCFunc[entryFree](l.ctx, symEntryFree)
		free(inner)
	})

	l.entry = entry
	return true
}

func (l *Lister) Entry() *Entry {
	return l.entry
}

type Entry struct {
	ctx   context.Context
	inner *opendalEntry
	err   error
}

func (e *Entry) Error() error {
	return e.err
}

func (e *Entry) Name() string {
	name := getCFunc[entryName](e.ctx, symEntryName)
	return name(e.inner)
}

func (e *Entry) Path() string {
	path := getCFunc[entryPath](e.ctx, symEntryPath)
	return path(e.inner)
}

const symOperatorList = "opendal_operator_list"

type operatorList func(op *opendalOperator, path string) (*opendalLister, error)

func withOperatorList(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultList,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorList)
	if err != nil {
		return
	}
	var cFn operatorList = func(op *opendalOperator, path string) (*opendalLister, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result opendalResultList
		ffi.Call(&cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&op), unsafe.Pointer(&bytePath))
		if result.err != nil {
			return nil, parseError(ctx, result.err)
		}
		return result.lister, nil
	}
	newCtx = context.WithValue(ctx, symOperatorList, cFn)
	return
}

const symListerFree = "opendal_lister_free"

type listerFree func(l *opendalLister)

func withListerFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symListerFree)
	if err != nil {
		return
	}
	var cFn listerFree = func(l *opendalLister) {
		ffi.Call(&cif, fn, nil, unsafe.Pointer(&l))
	}
	newCtx = context.WithValue(ctx, symListerFree, cFn)
	return
}

const symListerNext = "opendal_lister_next"

type listerNext func(l *opendalLister) (*opendalEntry, error)

func withListerNext(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&typeResultListerNext,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symListerNext)
	if err != nil {
		return
	}
	var cFn listerNext = func(l *opendalLister) (*opendalEntry, error) {
		var result opendalResultListerNext
		ffi.Call(&cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&l))
		if result.err != nil {
			return nil, parseError(ctx, result.err)
		}
		return result.entry, nil
	}
	newCtx = context.WithValue(ctx, symListerNext, cFn)
	return
}

const symEntryFree = "opendal_entry_free"

type entryFree func(e *opendalEntry)

func withEntryFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symEntryFree)
	if err != nil {
		return
	}
	var cFn entryFree = func(e *opendalEntry) {
		ffi.Call(&cif, fn, nil, unsafe.Pointer(&e))
	}
	newCtx = context.WithValue(ctx, symEntryFree, cFn)
	return
}

const symEntryName = "opendal_entry_name"

type entryName func(e *opendalEntry) string

func withEntryName(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symEntryName)
	if err != nil {
		return
	}
	var cFn entryName = func(e *opendalEntry) string {
		var bytePtr *byte
		ffi.Call(&cif, fn, unsafe.Pointer(&bytePtr), unsafe.Pointer(&e))
		return unix.BytePtrToString(bytePtr)
	}
	newCtx = context.WithValue(ctx, symEntryName, cFn)
	return
}

const symEntryPath = "opendal_entry_path"

type entryPath func(e *opendalEntry) string

func withEntryPath(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symEntryPath)
	if err != nil {
		return
	}
	var cFn entryPath = func(e *opendalEntry) string {
		var bytePtr *byte
		ffi.Call(&cif, fn, unsafe.Pointer(&bytePtr), unsafe.Pointer(&e))
		return unix.BytePtrToString(bytePtr)
	}
	newCtx = context.WithValue(ctx, symEntryPath, cFn)
	return
}
