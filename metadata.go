package opendal

import (
	"context"
	"errors"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

type Metadata struct {
	ctx context.Context

	inner *meta
}

func newMetadata(ctx context.Context, inner *meta) *Metadata {
	m := &Metadata{
		ctx:   ctx,
		inner: inner,
	}
	runtime.SetFinalizer(m, func(_ *Metadata) {
		free := getCFn[metadataFree](ctx, cFnMetadataFree)
		free(inner)
	})
	return m
}

func (m *Metadata) ContentLength() uint64 {
	length := getCFn[metadataContentLength](m.ctx, cFnMetadataContentLength)
	return length(m.inner)
}

func (m *Metadata) IsFile() bool {
	isFile := getCFn[metadataIsFile](m.ctx, cFnMetadataIsFile)
	return isFile(m.inner)
}

type metadataContentLength func(m *meta) uint64

const cFnMetadataContentLength = "opendal_metadata_content_length"

func metadataContentLengthRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint64,
		&typeMetadataPointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataContentLength)
	var cFn metadataContentLength = func(m *meta) uint64 {
		length := uint64(0)
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&length),
			unsafe.Pointer(&m),
		)
		return length
	}
	newCtx = context.WithValue(ctx, cFnMetadataContentLength, cFn)
	return
}

type metadataIsFile func(m *meta) bool

const cFnMetadataIsFile = "opendal_metadata_is_file"

func metadataIsFileRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint32,
		&typeMetadataPointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataIsFile)
	var cFn metadataIsFile = func(m *meta) bool {
		var result uint32
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
	newCtx = context.WithValue(ctx, cFnMetadataIsFile, cFn)
	return
}

type metadataFree func(m *meta)

const cFnMetadataFree = "opendal_metadata_free"

func metadataFreeRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&typeMetadataPointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataFree)
	var cFn metadataFree = func(m *meta) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&m),
		)
	}
	newCtx = context.WithValue(ctx, cFnMetadataFree, cFn)
	return
}
