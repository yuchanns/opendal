package opendal

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Info() *OperatorInfo {
	newInfo := getFFI[operatorInfoNew](op.ctx, symOperatorInfoNew)
	inner := newInfo(op.inner)
	info := &OperatorInfo{inner: inner, op: op}

	runtime.SetFinalizer(info, func(_ *OperatorInfo) {
		free := getFFI[operatorInfoFree](op.ctx, symOperatorInfoFree)
		free(inner)
	})
	return info
}

type OperatorInfo struct {
	inner *opendalOperatorInfo
	op    *Operator // hold the op pointer to ensure it is gc after OperatorInfo instance.
}

func (i *OperatorInfo) GetFullCapability() *Capability {
	getCap := getFFI[operatorInfoGetFullCapability](i.op.ctx, symOperatorInfoGetFullCapability)
	cap := getCap(i.inner)
	return &Capability{inner: cap}
}

func (i *OperatorInfo) GetNativeCapability() *Capability {
	getCap := getFFI[operatorInfoGetNativeCapability](i.op.ctx, symOperatorInfoGetNativeCapability)
	cap := getCap(i.inner)
	return &Capability{inner: cap}
}

func (i *OperatorInfo) GetScheme() string {
	getScheme := getFFI[operatorInfoGetScheme](i.op.ctx, symOperatorInfoGetScheme)
	return getScheme(i.inner)
}

func (i *OperatorInfo) GetRoot() string {
	getRoot := getFFI[operatorInfoGetRoot](i.op.ctx, symOperatorInfoGetRoot)
	return getRoot(i.inner)
}

func (i *OperatorInfo) GetName() string {
	getName := getFFI[operatorInfoGetName](i.op.ctx, symOperatorInfoGetName)
	return getName(i.inner)
}

type Capability struct {
	inner *opendalCapability
}

func (c *Capability) Stat() bool {
	return c.inner.stat == 1
}

func (c *Capability) StatWithIfmatch() bool {
	return c.inner.statWithIfmatch == 1
}

func (c *Capability) StatWithIfNoneMatch() bool {
	return c.inner.statWithIfNoneMatch == 1
}

func (c *Capability) Read() bool {
	return c.inner.read == 1
}

func (c *Capability) ReadWithIfmatch() bool {
	return c.inner.readWithIfmatch == 1
}

func (c *Capability) ReadWithIfMatchNone() bool {
	return c.inner.readWithIfMatchNone == 1
}

func (c *Capability) ReadWithOverrideCacheControl() bool {
	return c.inner.readWithOverrideCacheControl == 1
}

func (c *Capability) ReadWithOverrideContentDisposition() bool {
	return c.inner.readWithOverrideContentDisposition == 1
}

func (c *Capability) ReadWithOverrideContentType() bool {
	return c.inner.readWithOverrideContentType == 1
}

func (c *Capability) Write() bool {
	return c.inner.write == 1
}

func (c *Capability) WriteCanMulti() bool {
	return c.inner.writeCanMulti == 1
}

func (c *Capability) WriteCanEmpty() bool {
	return c.inner.writeCanEmpty == 1
}

func (c *Capability) WriteCanAppend() bool {
	return c.inner.writeCanAppend == 1
}

func (c *Capability) WriteWithContentType() bool {
	return c.inner.writeWithContentType == 1
}

func (c *Capability) WriteWithContentDisposition() bool {
	return c.inner.writeWithContentDisposition == 1
}

func (c *Capability) WriteWithCacheControl() bool {
	return c.inner.writeWithCacheControl == 1
}

func (c *Capability) WriteMultiMaxSize() uint {
	return c.inner.writeMultiMaxSize
}

func (c *Capability) WriteMultiMinSize() uint {
	return c.inner.writeMultiMinSize
}

func (c *Capability) WriteMultiAlignSize() uint {
	return c.inner.writeMultiAlignSize
}

func (c *Capability) WriteTotalMaxSize() bool {
	return c.inner.writeTotalMaxSize == 1
}

func (c *Capability) CreateDir() bool {
	return c.inner.createDir == 1
}

func (c *Capability) Delete() bool {
	return c.inner.delete == 1
}

func (c *Capability) Copy() bool {
	return c.inner.copy == 1
}

func (c *Capability) Rename() bool {
	return c.inner.rename == 1
}

func (c *Capability) List() bool {
	return c.inner.list == 1
}

func (c *Capability) ListWithLimit() bool {
	return c.inner.listWithLimit == 1
}

func (c *Capability) ListWithStartAfter() bool {
	return c.inner.listWithStartAfter == 1
}

func (c *Capability) ListWithRecursive() bool {
	return c.inner.listWithRecursive == 1
}

func (c *Capability) Presign() bool {
	return c.inner.presign == 1
}

func (c *Capability) PresignRead() bool {
	return c.inner.presignRead == 1
}

func (c *Capability) PresignStat() bool {
	return c.inner.presignStat == 1
}

func (c *Capability) PresignWrite() bool {
	return c.inner.presignWrite == 1
}

func (c *Capability) Batch() bool {
	return c.inner.batch == 1
}

func (c *Capability) BatchDelete() bool {
	return c.inner.batchDelete == 1
}

func (c *Capability) BatchMaxOperations() uint {
	return c.inner.batchMaxOperations
}

func (c *Capability) Blocking() bool {
	return c.inner.blocking == 1
}

const symOperatorInfoNew = "opendal_operator_info_new"

type operatorInfoNew func(op *opendalOperator) *opendalOperatorInfo

var withOperatorInfoNew = withFFI(ffiOpts{
	sym:    symOperatorInfoNew,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoNew {
	return func(op *opendalOperator) *opendalOperatorInfo {
		var result *opendalOperatorInfo
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
		)
		return result
	}
})

const symOperatorInfoFree = "opendal_operator_info_free"

type operatorInfoFree func(info *opendalOperatorInfo)

var withOperatorInfoFree = withFFI(ffiOpts{
	sym:    symOperatorInfoFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoFree {
	return func(info *opendalOperatorInfo) {
		ffiCall(
			nil,
			unsafe.Pointer(&info),
		)
	}
})

const symOperatorInfoGetFullCapability = "opendal_operator_info_get_full_capability"

type operatorInfoGetFullCapability func(info *opendalOperatorInfo) *opendalCapability

var withOperatorInfoGetFullCapability = withFFI(ffiOpts{
	sym:    symOperatorInfoGetFullCapability,
	rType:  &typeCapability,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetFullCapability {
	return func(info *opendalOperatorInfo) *opendalCapability {
		var cap opendalCapability
		ffiCall(
			unsafe.Pointer(&cap),
			unsafe.Pointer(&info),
		)
		return &cap
	}
})

const symOperatorInfoGetNativeCapability = "opendal_operator_info_get_native_capability"

type operatorInfoGetNativeCapability func(info *opendalOperatorInfo) *opendalCapability

var withOperatorInfoGetNativeCapability = withFFI(ffiOpts{
	sym:    symOperatorInfoGetNativeCapability,
	rType:  &typeCapability,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetNativeCapability {
	return func(info *opendalOperatorInfo) *opendalCapability {
		var cap opendalCapability
		ffiCall(
			unsafe.Pointer(&cap),
			unsafe.Pointer(&info),
		)
		return &cap
	}
})

const symOperatorInfoGetScheme = "opendal_operator_info_get_scheme"

type operatorInfoGetScheme func(info *opendalOperatorInfo) string

var withOperatorInfoGetScheme = withFFI(ffiOpts{
	sym:    symOperatorInfoGetScheme,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetScheme {
	return func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&info),
		)
		return unix.BytePtrToString(bytePtr)
	}
})

const symOperatorInfoGetRoot = "opendal_operator_info_get_root"

type operatorInfoGetRoot func(info *opendalOperatorInfo) string

var withOperatorInfoGetRoot = withFFI(ffiOpts{
	sym:    symOperatorInfoGetRoot,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetRoot {
	return func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&info),
		)
		return unix.BytePtrToString(bytePtr)
	}
})

const symOperatorInfoGetName = "opendal_operator_info_get_name"

type operatorInfoGetName func(info *opendalOperatorInfo) string

var withOperatorInfoGetName = withFFI(ffiOpts{
	sym:    symOperatorInfoGetName,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetName {
	return func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&info),
		)
		return unix.BytePtrToString(bytePtr)
	}
})
