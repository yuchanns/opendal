package opendal

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func newOperator(libopendal uintptr, scheme Schemer, opts *operatorOptions) (op *operator, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultOperatorNew,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_new")
	if err != nil {
		return
	}
	fn := func(scheme Schemer, opts *operatorOptions) (*operator, error) {
		byteName, err := unix.BytePtrFromString(scheme.Scheme())
		if err != nil {
			return nil, err
		}
		var result resultOperatorNew
		ffi.Call(&cif, sym, unsafe.Pointer(&result), unsafe.Pointer(&byteName), unsafe.Pointer(opts))
		if result.error != nil {
			return nil, result.error
		}
		return result.op, nil
	}
	return fn(scheme, opts)
}

type operatorOptions struct {
	inner uintptr
}

func newOperatorOptions(libopendal uintptr) (*operatorOptions, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 0,
		&ffi.TypePointer,
	); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_new")
	if err != nil {
		return nil, err
	}
	fn := func() operatorOptions {
		var opts operatorOptions
		ffi.Call(&cif, sym, unsafe.Pointer(&opts))
		return opts
	}
	opts := fn()
	return &opts, nil
}

func operatorOptionsSet(libopendal uintptr, opts *operatorOptions, key, value string) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 3,
		&ffi.TypeVoid,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_set")
	if err != nil {
		return err
	}
	fn := func(opts *operatorOptions, key, value string) error {
		byteKey, err := unix.BytePtrFromString(key)
		if err != nil {
			return err
		}
		byteValue, err := unix.BytePtrFromString(value)
		if err != nil {
			return err
		}
		ffi.Call(&cif, sym, nil, unsafe.Pointer(opts), unsafe.Pointer(&byteKey), unsafe.Pointer(&byteValue))
		return nil
	}
	return fn(opts, key, value)
}

func operatorOptionsFree(libopendal uintptr, opts *operatorOptions) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_free")
	if err != nil {
		return err
	}
	fn := func(opts operatorOptions) {
		ffi.Call(&cif, sym, nil, unsafe.Pointer(&opts))
	}
	fn(*opts)
	return nil
}
