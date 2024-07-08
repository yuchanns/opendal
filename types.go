package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

var (
	typeResultOperatorNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeResultRead = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeBytes = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypeUint64,
			nil,
		}[0],
	}

	typeResultStat = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}
)

type resultOperatorNew struct {
	op    *opendalOperator
	error *opendalError
}

type opendalOperator struct {
	ptr uintptr
}

type resultRead struct {
	data  *opendalBytes
	error *opendalError
}

type resultStat struct {
	meta  *opendalMetadata
	error *opendalError
}

type opendalMetadata struct {
	inner uintptr
}

type opendalBytes struct {
	data *byte
	len  uintptr
}

type opendalError struct {
	code    int32
	message opendalBytes
}

func freeBytes(ctx context.Context, b *opendalBytes) {
	if b == nil || b.len == 0 {
		return
	}
	free := getCFunc[bytesFree](ctx, symBytesFree)
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

const symBytesFree = "opendal_bytes_free"

func withBytesFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symBytesFree)
	if err != nil {
		return
	}
	var cFn bytesFree = func(b *opendalBytes) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&b),
		)
	}
	newCtx = context.WithValue(ctx, symBytesFree, cFn)
	return
}
