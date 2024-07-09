package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Read(path string) ([]byte, error) {
	read := getCFunc[operatorRead](o.ctx, symOperatorRead)
	bytes, err := read(o.inner, path)
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if len(data) > 0 {
		free := getCFunc[bytesFree](o.ctx, symBytesFree)
		free(bytes)

	}
	return data, nil
}

type operatorRead func(op *opendalOperator, path string) (*opendalBytes, error)

const symOperatorRead = "opendal_operator_read"

func withOperatorRead(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
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
	fn, err := purego.Dlsym(libopendal, symOperatorRead)
	if err != nil {
		return
	}
	var cFn operatorRead = func(op *opendalOperator, path string) (*opendalBytes, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultRead
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return result.data, parseError(ctx, result.error)
	}
	newCtx = context.WithValue(ctx, symOperatorRead, cFn)
	return
}
