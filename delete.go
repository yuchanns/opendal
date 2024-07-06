package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Delete(path string) error {
	delete := getCFn[operatorDelete](o.ctx, cFnOperatorDelete)
	return delete(o.inner, path)
}

type operatorDelete func(op *operator, path string) error

const cFnOperatorDelete = "opendal_operator_delete"

func operatorDeleteRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnOperatorDelete)
	var cFn operatorDelete = func(op *operator, path string) error {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return e.parse()
	}
	newCtx = context.WithValue(ctx, cFnOperatorDelete, cFn)
	return
}
