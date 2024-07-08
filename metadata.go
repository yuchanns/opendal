package opendal

import (
	"context"
	"errors"
	"runtime"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

type Metadata struct {
	ctx context.Context

	inner *opendalMetadata
}

func newMetadata(ctx context.Context, inner *opendalMetadata) *Metadata {
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

func (m *Metadata) IsDir() bool {
	isDir := getCFn[metadataIsDir](m.ctx, cFnMetadataIsDir)
	return isDir(m.inner)
}

func (m *Metadata) LastModified() time.Time {
	lastModifiedMs := getCFn[metadataLastModifiedMs](m.ctx, cFnMetadataLastModifiedMs)
	ms := lastModifiedMs(m.inner)
	if ms == -1 {
		var zeroTime time.Time
		return zeroTime
	}
	return time.UnixMilli(ms)
}

type metadataContentLength func(m *opendalMetadata) uint64

const cFnMetadataContentLength = "opendal_metadata_content_length"

func metadataContentLengthRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint64,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataContentLength)
	if err != nil {
		return
	}
	var cFn metadataContentLength = func(m *opendalMetadata) uint64 {
		var length uint64
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

type metadataIsFile func(m *opendalMetadata) bool

const cFnMetadataIsFile = "opendal_metadata_is_file"

func metadataIsFileRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint32,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataIsFile)
	if err != nil {
		return
	}
	var cFn metadataIsFile = func(m *opendalMetadata) bool {
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

type metadataIsDir func(m *opendalMetadata) bool

const cFnMetadataIsDir = "opendal_metadata_is_dir"

func metadataIsDirRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint32,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataIsDir)
	if err != nil {
		return
	}
	var cFn metadataIsDir = func(m *opendalMetadata) bool {
		var result uint32
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
	newCtx = context.WithValue(ctx, cFnMetadataIsDir, cFn)
	return
}

type metadataLastModifiedMs func(m *opendalMetadata) int64

const cFnMetadataLastModifiedMs = "opendal_metadata_last_modified_ms"

func metadataLastModifiedMsRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeSint64,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataLastModifiedMs)
	if err != nil {
		return
	}
	var cFn metadataLastModifiedMs = func(m *opendalMetadata) int64 {
		var result int64
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result
	}
	newCtx = context.WithValue(ctx, cFnMetadataLastModifiedMs, cFn)
	return
}

type metadataFree func(m *opendalMetadata)

const cFnMetadataFree = "opendal_metadata_free"

func metadataFreeRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnMetadataFree)
	if err != nil {
		return
	}
	var cFn metadataFree = func(m *opendalMetadata) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&m),
		)
	}
	newCtx = context.WithValue(ctx, cFnMetadataFree, cFn)
	return
}
