package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Delete(path string) error {
	delete := getFFI[operatorDelete](op.ctx, symOperatorDelete)
	return delete(op.inner, path)
}

type operatorDelete func(op *opendalOperator, path string) error

const symOperatorDelete = "opendal_operator_delete"

func withOperatorDelete(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorDelete,
		nArgs:  2,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorDelete {
		return func(op *opendalOperator, path string) error {
			bytePath, err := unix.BytePtrFromString(path)
			if err != nil {
				return err
			}
			var e *opendalError
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&e),
				unsafe.Pointer(&op),
				unsafe.Pointer(&bytePath),
			)
			return parseError(ctx, e)
		}
	})
}
