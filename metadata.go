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

var withMetaContentLength = withFFI(ffiOpts{
	sym:    symMetadataContentLength,
	rType:  &ffi.TypeUint64,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) metaContentLength {
	return func(m *opendalMetadata) uint64 {
		var length uint64
		ffiCall(
			unsafe.Pointer(&length),
			unsafe.Pointer(&m),
		)
		return length
	}
})

type metaIsFile func(m *opendalMetadata) bool

const symMetadataIsFile = "opendal_metadata_is_file"

var withMetaIsFile = withFFI(ffiOpts{
	sym:    symMetadataIsFile,
	rType:  &ffi.TypeUint8,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) metaIsFile {
	return func(m *opendalMetadata) bool {
		var result uint8
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
})

type metaIsDir func(m *opendalMetadata) bool

const symMetadataIsDir = "opendal_metadata_is_dir"

var withMetaIsDir = withFFI(ffiOpts{
	sym:    symMetadataIsDir,
	rType:  &ffi.TypeUint8,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) metaIsDir {
	return func(m *opendalMetadata) bool {
		var result uint8
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
})

type metaLastModified func(m *opendalMetadata) int64

const symMetadataLastModified = "opendal_metadata_last_modified_ms"

var withMetaLastModified = withFFI(ffiOpts{
	sym:    symMetadataLastModified,
	rType:  &ffi.TypeSint64,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) metaLastModified {
	return func(m *opendalMetadata) int64 {
		var result int64
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result
	}
})

type metaFree func(m *opendalMetadata)

const symMetadataFree = "opendal_metadata_free"

var withMetaFree = withFFI(ffiOpts{
	sym:    symMetadataFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) metaFree {
	return func(m *opendalMetadata) {
		ffiCall(
			nil,
			unsafe.Pointer(&m),
		)
	}
})
