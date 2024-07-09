package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Stat(path string) (*Metadata, error) {
	stat := getCFunc[operatorStat](op.ctx, symOperatorStat)
	meta, err := stat(op.inner, path)
	if err != nil {
		return nil, err
	}
	return newMetadata(op.ctx, meta), nil
}

func (op *Operator) IsExist(path string) (bool, error) {
	isExist := getCFunc[operatorIsExist](op.ctx, symOperatorIsExist)
	return isExist(op.inner, path)
}

const symOperatorStat = "opendal_operator_stat"

type operatorStat func(op *opendalOperator, path string) (*opendalMetadata, error)

func withOperatorStat(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorStat,
		nArgs:  2,
		rType:  &typeResultStat,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorStat {
		return func(op *opendalOperator, path string) (*opendalMetadata, error) {
			bytePath, err := unix.BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var result resultStat
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&result),
				unsafe.Pointer(&op),
				unsafe.Pointer(&bytePath),
			)
			if result.error != nil {
				return nil, parseError(ctx, result.error)
			}
			return result.meta, nil
		}
	})
}

const symOperatorIsExist = "opendal_operator_is_exist"

type operatorIsExist func(op *opendalOperator, path string) (bool, error)

func withOperatorIsExists(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorIsExist,
		nArgs:  2,
		rType:  &typeResultIsExist,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorIsExist {
		return func(op *opendalOperator, path string) (bool, error) {
			bytePath, err := unix.BytePtrFromString(path)
			if err != nil {
				return false, err
			}
			var result resultIsExist
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&result),
				unsafe.Pointer(&op),
				unsafe.Pointer(&bytePath),
			)
			if result.error != nil {
				return false, parseError(ctx, result.error)
			}
			return result.is_exist == 1, nil
		}
	})
}
