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
	meta, err := stat(op.inner, path)
	if err != nil {
		return nil, err
	}
	return newMetadata(op.ctx, meta), nil
}

type operatorStat func(op *opendalOperator, path string) (*opendalMetadata, error)

const cFnOperatorStat = "opendal_operator_stat"

func operatorStatRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
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
	fn, err := purego.Dlsym(libopendal, cFnOperatorRead)
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
		return (*opendalMetadata)(unsafe.Pointer(uintptr(unsafe.Pointer(&result)) + unsafe.Offsetof(result.meta))), nil
	}
	newCtx = context.WithValue(ctx, cFnOperatorStat, cFn)
	return
}
