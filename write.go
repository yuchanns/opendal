package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Write(path string, data []byte) error {
	write := getCFn[operatorWrite](o.ctx, cFnOperatorWrite)
	return write(o.inner, path, data)
}

const cFnOperatorWrite = "opendal_operator_write"

type operatorWrite func(op *operator, path string, data []byte) error

func operatorWriteRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 3,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&typeBytes,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnOperatorWrite)
	if err != nil {
		return
	}
	var cFn operatorWrite = func(op *operator, path string, data []byte) error {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return err
		}
		bytes := toOpendalBytes(data)
		if len(data) > 0 {
			bytes.data = &data[0]
		}
		var e *opendalError
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&bytes),
		)
		return parseError(ctx, e)
	}
	newCtx = context.WithValue(ctx, cFnOperatorWrite, cFn)
	return
}
