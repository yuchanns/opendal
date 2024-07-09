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
		free := getCFunc[metaFree](ctx, symMetadataFree)
		free(inner)
	})
	return m
}

func (m *Metadata) ContentLength() uint64 {
	length := getCFunc[metaContentLength](m.ctx, symMetadataContentLength)
	return length(m.inner)
}

func (m *Metadata) IsFile() bool {
	isFile := getCFunc[metaIsFile](m.ctx, symMetadataIsFile)
	return isFile(m.inner)
}

func (m *Metadata) IsDir() bool {
	isDir := getCFunc[metaIsDir](m.ctx, symMetadataIsDir)
	return isDir(m.inner)
}

func (m *Metadata) LastModified() time.Time {
	lastModifiedMs := getCFunc[metaLastModified](m.ctx, symMetadataLastModified)
	ms := lastModifiedMs(m.inner)
	if ms == -1 {
		var zeroTime time.Time
		return zeroTime
	}
	return time.UnixMilli(ms)
}

type metaContentLength func(m *opendalMetadata) uint64

const symMetadataContentLength = "opendal_metadata_content_length"

func withMetaContentLength(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint64,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symMetadataContentLength)
	if err != nil {
		return
	}
	var cFn metaContentLength = func(m *opendalMetadata) uint64 {
		var length uint64
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&length),
			unsafe.Pointer(&m),
		)
		return length
	}
	newCtx = context.WithValue(ctx, symMetadataContentLength, cFn)
	return
}

type metaIsFile func(m *opendalMetadata) bool

const symMetadataIsFile = "opendal_metadata_is_file"

func withMetaIsFile(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint8,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symMetadataIsFile)
	if err != nil {
		return
	}
	var cFn metaIsFile = func(m *opendalMetadata) bool {
		var result uint8
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
	newCtx = context.WithValue(ctx, symMetadataIsFile, cFn)
	return
}

type metaIsDir func(m *opendalMetadata) bool

const symMetadataIsDir = "opendal_metadata_is_dir"

func withMetaIsDir(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeUint8,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symMetadataIsDir)
	if err != nil {
		return
	}
	var cFn metaIsDir = func(m *opendalMetadata) bool {
		var result uint8
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
	newCtx = context.WithValue(ctx, symMetadataIsDir, cFn)
	return
}

type metaLastModified func(m *opendalMetadata) int64

const symMetadataLastModified = "opendal_metadata_last_modified_ms"

func withMetaLastModified(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeSint64,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symMetadataLastModified)
	if err != nil {
		return
	}
	var cFn metaLastModified = func(m *opendalMetadata) int64 {
		var result int64
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result
	}
	newCtx = context.WithValue(ctx, symMetadataLastModified, cFn)
	return
}

type metaFree func(m *opendalMetadata)

const symMetadataFree = "opendal_metadata_free"

func withMetaFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symMetadataFree)
	if err != nil {
		return
	}
	var cFn metaFree = func(m *opendalMetadata) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&m),
		)
	}
	newCtx = context.WithValue(ctx, symMetadataFree, cFn)
	return
}
