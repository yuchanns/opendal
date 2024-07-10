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

var withOperatorDelete = withFFI(ffiOpts{
	sym:    symOperatorDelete,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorDelete {
	return func(op *opendalOperator, path string) error {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return parseError(ctx, e)
	}
})
