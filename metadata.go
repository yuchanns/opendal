package opendal

import (
	"context"
	"runtime"
	"time"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

type Metadata struct {
	inner *opendalMetadata
	op    *Operator // hold the op pointer to ensure it is gc after Metadata instance.
}

func newMetadata(op *Operator, inner *opendalMetadata) *Metadata {
	m := &Metadata{
		op:    op,
		inner: inner,
	}
	runtime.SetFinalizer(m, func(_ *Metadata) {
		free := getFFI[metaFree](op.ctx, symMetadataFree)
		free(inner)
	})
	return m
}

func (m *Metadata) ContentLength() uint64 {
	length := getFFI[metaContentLength](m.op.ctx, symMetadataContentLength)
	return length(m.inner)
}

func (m *Metadata) IsFile() bool {
	isFile := getFFI[metaIsFile](m.op.ctx, symMetadataIsFile)
	return isFile(m.inner)
}

func (m *Metadata) IsDir() bool {
	isDir := getFFI[metaIsDir](m.op.ctx, symMetadataIsDir)
	return isDir(m.inner)
}

func (m *Metadata) LastModified() time.Time {
	lastModifiedMs := getFFI[metaLastModified](m.op.ctx, symMetadataLastModified)
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
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symMetadataContentLength,
		nArgs:  1,
		rType:  &ffi.TypeUint64,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) metaContentLength {
		return func(m *opendalMetadata) uint64 {
			var length uint64
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&length),
				unsafe.Pointer(&m),
			)
			return length
		}
	})
}

type metaIsFile func(m *opendalMetadata) bool

const symMetadataIsFile = "opendal_metadata_is_file"

func withMetaIsFile(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symMetadataIsFile,
		nArgs:  1,
		rType:  &ffi.TypeUint8,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) metaIsFile {
		return func(m *opendalMetadata) bool {
			var result uint8
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&result),
				unsafe.Pointer(&m),
			)
			return result == 1
		}
	})
}

type metaIsDir func(m *opendalMetadata) bool

const symMetadataIsDir = "opendal_metadata_is_dir"

func withMetaIsDir(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symMetadataIsDir,
		nArgs:  1,
		rType:  &ffi.TypeUint8,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) metaIsDir {
		return func(m *opendalMetadata) bool {
			var result uint8
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&result),
				unsafe.Pointer(&m),
			)
			return result == 1
		}
	})
}

type metaLastModified func(m *opendalMetadata) int64

const symMetadataLastModified = "opendal_metadata_last_modified_ms"

func withMetaLastModified(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symMetadataLastModified,
		nArgs:  1,
		rType:  &ffi.TypeSint64,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) metaLastModified {
		return func(m *opendalMetadata) int64 {
			var result int64
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&result),
				unsafe.Pointer(&m),
			)
			return result
		}
	})
}

type metaFree func(m *opendalMetadata)

const symMetadataFree = "opendal_metadata_free"

func withMetaFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symMetadataFree,
		nArgs:  1,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) metaFree {
		return func(m *opendalMetadata) {
			ffi.Call(
				cif, fn,
				nil,
				unsafe.Pointer(&m),
			)
		}
	})
}
