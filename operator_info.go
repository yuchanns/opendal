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

func (op *Operator) Info() *OperatorInfo {
	newInfo := getCFunc[operatorInfoNew](op.ctx, symOperatorInfoNew)
	inner := newInfo(op.inner)
	info := &OperatorInfo{inner: inner, ctx: op.ctx}

	runtime.SetFinalizer(info, func(_ *OperatorInfo) {
		free := getCFunc[operatorInfoFree](op.ctx, symOperatorInfoFree)
		free(inner)
	})
	return info
}

type OperatorInfo struct {
	inner *opendalOperatorInfo
	ctx   context.Context
}

func (i *OperatorInfo) GetFullCapability() *Capability {
	getCap := getCFunc[operatorInfoGetFullCapability](i.ctx, symOperatorInfoGetFullCapability)
	cap := getCap(i.inner)
	return &Capability{inner: cap}
}

func (i *OperatorInfo) GetNativeCapability() *Capability {
	getCap := getCFunc[operatorInfoGetNativeCapability](i.ctx, symOperatorInfoGetNativeCapability)
	cap := getCap(i.inner)
	return &Capability{inner: cap}
}

func (i *OperatorInfo) GetScheme() string {
	getScheme := getCFunc[operatorInfoGetScheme](i.ctx, symOperatorInfoGetScheme)
	return getScheme(i.inner)
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

func withOperatorInfoNew(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorInfoNew)
	if err != nil {
		return
	}
	var cFn operatorInfoNew = func(op *opendalOperator) *opendalOperatorInfo {
		var result *opendalOperatorInfo
		ffi.Call(&cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&op))
		return result
	}
	newCtx = context.WithValue(ctx, symOperatorInfoNew, cFn)
	return
}

const symOperatorInfoFree = "opendal_operator_info_free"

type operatorInfoFree func(self *opendalOperatorInfo)

func withOperatorInfoFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorInfoFree)
	if err != nil {
		return
	}
	var cFn operatorInfoFree = func(info *opendalOperatorInfo) {
		ffi.Call(&cif, fn, nil, unsafe.Pointer(&info))
		return
	}
	newCtx = context.WithValue(ctx, symOperatorInfoFree, cFn)
	return
}

const symOperatorInfoGetFullCapability = "opendal_operator_info_get_full_capability"

type operatorInfoGetFullCapability func(self *opendalOperatorInfo) *opendalCapability

func withOperatorInfoGetFullCapability(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&typeCapability,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorInfoGetFullCapability)
	if err != nil {
		return
	}
	var cFn operatorInfoGetFullCapability = func(info *opendalOperatorInfo) *opendalCapability {
		var cap opendalCapability
		ffi.Call(&cif, fn, unsafe.Pointer(&cap), unsafe.Pointer(&info))
		return &cap
	}
	newCtx = context.WithValue(ctx, symOperatorInfoGetFullCapability, cFn)
	return
}

const symOperatorInfoGetNativeCapability = "opendal_operator_info_get_native_capability"

type operatorInfoGetNativeCapability func(self *opendalOperatorInfo) *opendalCapability

func withOperatorInfoGetNativeCapability(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&typeCapability,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorInfoGetNativeCapability)
	if err != nil {
		return
	}
	var cFn operatorInfoGetNativeCapability = func(info *opendalOperatorInfo) *opendalCapability {
		var cap opendalCapability
		ffi.Call(&cif, fn, unsafe.Pointer(&cap), unsafe.Pointer(&info))
		return &cap
	}
	newCtx = context.WithValue(ctx, symOperatorInfoGetNativeCapability, cFn)
	return
}

const symOperatorInfoGetScheme = "opendal_operator_info_get_scheme"

type operatorInfoGetScheme func(self *opendalOperatorInfo) string

func withOperatorInfoGetScheme(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorInfoGetScheme)
	if err != nil {
		return
	}
	var cFn operatorInfoGetScheme = func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffi.Call(&cif, fn, unsafe.Pointer(&bytePtr), unsafe.Pointer(&info))
		return unix.BytePtrToString(bytePtr)
	}
	newCtx = context.WithValue(ctx, symOperatorInfoGetScheme, cFn)
	return
}
