package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func newOperator(ctx context.Context, libopendal uintptr, scheme Schemer, opts *operatorOptions) (op *opendalOperator, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultOperatorNew,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	fn, err := purego.Dlsym(libopendal, "opendal_operator_new")
	if err != nil {
		return
	}
	var byteName *byte
	byteName, err = unix.BytePtrFromString(scheme.Scheme())
	if err != nil {
		return
	}
	var result resultOperatorNew
	ffi.Call(&cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&byteName), unsafe.Pointer(&opts))
	if result.error != nil {
		err = parseError(ctx, result.error)
		return
	}
	op = result.op
	return
}

func operatorFree(libopendal uintptr, op *opendalOperator) (err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, "opendal_operator_free")
	if err != nil {
		return
	}
	ffi.Call(&cif, fn, nil, unsafe.Pointer(&op))
	return
}

type operatorOptions struct {
	inner uintptr
}

func newOperatorOptions(libopendal uintptr) (opts *operatorOptions, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 0,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, "opendal_operator_options_new")
	if err != nil {
		return
	}
	ffi.Call(&cif, fn, unsafe.Pointer(&opts))
	return
}

const symOperatorOptionSet = "opendal_operator_options_set"

type operatorOptionsSet func(opts *operatorOptions, key, value string) error

func withOperatorOptionsSet(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 3,
		&ffi.TypeVoid,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorOptionSet)
	if err != nil {
		return
	}
	var cFn operatorOptionsSet = func(opts *operatorOptions, key, value string) error {
		var (
			byteKey   *byte
			byteValue *byte
		)
		byteKey, err = unix.BytePtrFromString(key)
		if err != nil {
			return err
		}
		byteValue, err = unix.BytePtrFromString(value)
		if err != nil {
			return err
		}
		ffi.Call(&cif, fn, nil, unsafe.Pointer(&opts), unsafe.Pointer(&byteKey), unsafe.Pointer(&byteValue))
		return nil
	}
	newCtx = context.WithValue(ctx, symOperatorOptionSet, cFn)
	return
}

func operatorOptionsFree(libopendal uintptr, opts *operatorOptions) (err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		return errors.New(status.String())
	}
	fn, err := purego.Dlsym(libopendal, "opendal_operator_options_free")
	if err != nil {
		return err
	}
	ffi.Call(&cif, fn, nil, unsafe.Pointer(&opts))
	return
}
