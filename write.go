package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Write(path string, data []byte) error {
	write := getFFI[operatorWrite](op.ctx, symOperatorWrite)
	return write(op.inner, path, data)
}

func (op *Operator) CreateDir(path string) error {
	createDir := getFFI[operatorCreateDir](op.ctx, symOperatorCreateDir)
	return createDir(op.inner, path)
}

const symOperatorWrite = "opendal_operator_write"

type operatorWrite func(op *opendalOperator, path string, data []byte) error

func withOperatorWrite(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorWrite,
		nArgs:  3,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &typeBytes},
	}, func(cif *ffi.Cif, fn uintptr) operatorWrite {
		return func(op *opendalOperator, path string, data []byte) error {
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
				cif, fn,
				unsafe.Pointer(&e),
				unsafe.Pointer(&op),
				unsafe.Pointer(&bytePath),
				unsafe.Pointer(&bytes),
			)
			return parseError(ctx, e)
		}
	})
}

const symOperatorCreateDir = "opendal_operator_create_dir"

type operatorCreateDir func(op *opendalOperator, path string) error

func withOperatorCreateDir(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorCreateDir,
		nArgs:  2,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorCreateDir {
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
