package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

// Delete removes the file or directory at the specified path.
//
// # Parameters
//
//   - path: The path of the file or directory to delete.
//
// # Returns
//
//   - error: An error if the deletion fails, or nil if successful.
//
// # Note
//
// Use with caution as this operation is irreversible.
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
