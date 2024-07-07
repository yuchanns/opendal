package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Write(path string, data []byte) error {
	write := getCFn[operatorWrite](o.ctx, cFnOperatorWrite)
	return write(o.inner, path, data)
}

func (o *Operator) CreateDir(path string) error {
	createDir := getCFn[operatorCreateDir](o.ctx, cFnOperatorCreateDir)
	return createDir(o.inner, path)
}

const cFnOperatorWrite = "opendal_operator_write"

type operatorWrite func(op *opendalOperator, path string, data []byte) error

func operatorWriteRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 3,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&typeBytes,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, cFnOperatorWrite)
	if err != nil {
		return
	}
	var cFn operatorWrite = func(op *opendalOperator, path string, data []byte) error {
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
			&cif, fn,
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&bytes),
		)
		return parseError(ctx, e)
	}
	newCtx = context.WithValue(ctx, cFnOperatorWrite, cFn)
	return
}

const cFnOperatorCreateDir = "opendal_operator_create_dir"

type operatorCreateDir func(op *opendalOperator, path string) error

func operatorCreateDirRegister(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
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
	fn, err := purego.Dlsym(libopendal, cFnOperatorCreateDir)
	if err != nil {
		return
	}
	var cFn operatorCreateDir = func(op *opendalOperator, path string) error {
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
	newCtx = context.WithValue(ctx, cFnOperatorCreateDir, cFn)
	return
}
