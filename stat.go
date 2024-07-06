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
	stat := getCFn[operatorStat](op.ctx, cFnOperatorStat)
	result, err := stat(op.inner, path)
	if err != nil {
		return nil, err
	}
	return newMetadata(op.ctx, result.meta), nil
}

type operatorStat func(op *operator, path string) (*resultStat, error)

const cFnOperatorStat = "opendal_operator_stat"

func operatorStatRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
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
	var cFn operatorStat = func(op *operator, path string) (*resultStat, error) {
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
			return nil, result.error
		}
		return &result, nil
	}
	newCtx = context.WithValue(ctx, cFnOperatorStat, cFn)
	return
}
