package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

func freeBytes(ctx context.Context, b *opendalBytes) {
	if b == nil || b.len == 0 {
		return
	}
	free := getCFn[bytesFree](ctx, cFnBytesFree)
	free(b)
}

func toOpendalBytes(data []byte) opendalBytes {
	var ptr *byte
	l := len(data)
	if l > 0 {
		ptr = &data[0]
	}
	return opendalBytes{
		data: ptr,
		len:  uintptr(l),
	}
}

func parseBytes(b *opendalBytes) (data []byte) {
	if b == nil || b.len == 0 {
		return nil
	}
	data = make([]byte, b.len)
	copy(data, unsafe.Slice(b.data, b.len))
	return
}

type bytesFree func(b *opendalBytes)

const cFnBytesFree = "opendal_bytes_free"

func bytesFreeRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnBytesFree)
	var cFn bytesFree = func(b *opendalBytes) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&b),
		)
	}
	newCtx = context.WithValue(ctx, cFnBytesFree, cFn)
	return
}
