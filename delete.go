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
	delete := getCFunc[operatorDelete](o.ctx, symOperatorDelete)
	return delete(o.inner, path)
}

type operatorDelete func(op *opendalOperator, path string) error

const symOperatorDelete = "opendal_operator_delete"

func withOperatorDelete(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
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
	fn, err := purego.Dlsym(libopendal, symOperatorDelete)
	if err != nil {
		return
	}
	var cFn operatorDelete = func(op *opendalOperator, path string) error {
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
		return parseError(ctx, e)
	}
	newCtx = context.WithValue(ctx, symOperatorDelete, cFn)
	return
}
