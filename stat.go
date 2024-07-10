package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Stat(path string) (*Metadata, error) {
	stat := getFFI[operatorStat](op.ctx, symOperatorStat)
	meta, err := stat(op.inner, path)
	if err != nil {
		return nil, err
	}
	return newMetadata(op, meta), nil
}

func (op *Operator) IsExist(path string) (bool, error) {
	isExist := getFFI[operatorIsExist](op.ctx, symOperatorIsExist)
	return isExist(op.inner, path)
}

const symOperatorStat = "opendal_operator_stat"

type operatorStat func(op *opendalOperator, path string) (*opendalMetadata, error)

var withOperatorStat = withFFI(ffiOpts{
	sym:    symOperatorStat,
	rType:  &typeResultStat,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorStat {
	return func(op *opendalOperator, path string) (*opendalMetadata, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultStat
		ffiCall(
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

const symOperatorIsExist = "opendal_operator_is_exist"

type operatorIsExist func(op *opendalOperator, path string) (bool, error)

var withOperatorIsExists = withFFI(ffiOpts{
	sym:    symOperatorIsExist,
	rType:  &typeResultIsExist,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorIsExist {
	return func(op *opendalOperator, path string) (bool, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return false, err
		}
		var result resultIsExist
		ffiCall(
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
