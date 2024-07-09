package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
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
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultStat,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorStat)
	if err != nil {
		return
	}
	var cFn operatorStat = func(op *opendalOperator, path string) (*opendalMetadata, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultStat
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.meta, nil
	}
	newCtx = context.WithValue(ctx, symOperatorStat, cFn)
	return
}

const symOperatorIsExist = "opendal_operator_is_exist"

type operatorIsExist func(op *opendalOperator, path string) (bool, error)

func withOperatorIsExists(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultIsExist,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorIsExist)
	if err != nil {
		return
	}
	var cFn operatorIsExist = func(op *opendalOperator, path string) (bool, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return false, err
		}
		var result resultIsExist
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return false, parseError(ctx, result.error)
		}
		return result.is_exist == 1, nil
	}
	newCtx = context.WithValue(ctx, symOperatorIsExist, cFn)
	return
}
