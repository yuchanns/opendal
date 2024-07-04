package opendal

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

var (
	typeResultOperatorNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeOpenDALError = ffi.Type{
		Type: ffi.Pointer,
		Elements: &[]*ffi.Type{
			&ffi.TypeSint32,
			&typeBytes,
			nil,
		}[0],
	}

	typeResultRead = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeBytes = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypeUint64,
			nil,
		}[0],
	}
)

type resultOperatorNew struct {
	op    *RawOperator
	error *Error
}

type RawOperator struct {
	ptr uintptr
}

type Error struct {
	code    int32
	message openDALBytes
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d %s", e.code, e.message.data)
}

func (e *Error) Code() int32 {
	return e.code
}

func (e *Error) Message() string {
	return string(e.message.data)
}

type operatorOptions struct {
	inner uintptr
}

type resultRead struct {
	data  *openDALBytes
	error *Error
}

type openDALBytes struct {
	data []byte
}

func newRawOptions(libopendal uintptr) (*operatorOptions, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 0, &ffi.TypePointer); status != ffi.OK {
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
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 3, &ffi.TypeVoid, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer); status != ffi.OK {
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

func operatorOptionsFree(libopendal uintptr, opts operatorOptions) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 1, &ffi.TypeVoid, &ffi.TypePointer); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_free")
	if err != nil {
		return err
	}
	fn := func(opts operatorOptions) {
		ffi.Call(&cif, sym, nil, unsafe.Pointer(&opts))
	}
	fn(opts)
	return nil
}

func operatorWrite(op *Operator, path string, data []byte) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 3, &typeOpenDALError, &ffi.TypePointer, &ffi.TypePointer, &typeBytes); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(op.libopendal, "opendal_operator_write")
	if err != nil {
		return err
	}
	fn := func(op *RawOperator, path string, data []byte) error {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return err
		}
		bytes := openDALBytes{
			data: data,
		}
		var e *Error
		ffi.Call(
			&cif, sym,
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&bytes),
		)
		return e
	}

	return fn(op.inner, path, data)
}

func operatorRead(op *Operator, path string) ([]byte, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 2, &typeResultRead, &ffi.TypePointer, &ffi.TypePointer); status != ffi.OK {
		panic(status)
	}
	sym, err := purego.Dlsym(op.libopendal, "opendal_operator_read")
	fn := func(op *RawOperator, path string) (*resultRead, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var r resultRead
		ffi.Call(&cif, sym, unsafe.Pointer(&r), unsafe.Pointer(&op), unsafe.Pointer(&bytePath))
		return &r, nil
	}
	result, err := fn(op.inner, path)
	if err != nil {
		return nil, err
	}
	if result.error != nil {
		return nil, result.error
	}
	return result.data.data, nil
}
