package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Read(path string) ([]byte, error) {
	read := getCFn[operatorRead](o.ctx, cFnOperatorRead)
	return read(o.inner, path)
}

type operatorRead func(op *operator, path string) ([]byte, error)

const cFnOperatorRead = "opendal_operator_read"

func operatorReadRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultRead,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnOperatorRead)
	var cFn operatorRead = func(op *operator, path string) ([]byte, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultRead
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, result.error
		}
		return result.data.data, nil
	}
	newCtx = context.WithValue(ctx, cFnOperatorRead, cFn)
	return
}
