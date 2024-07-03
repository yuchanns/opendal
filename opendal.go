package opendal

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

var (
	TypeResultOperatorNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	TypeOpenDALError = ffi.Type{
		Type: ffi.Pointer,
		Elements: &[]*ffi.Type{
			&ffi.TypeSint32,
			&TypeBytes,
			nil,
		}[0],
	}

	TypeResultRead = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	TypeBytes = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypeUint64,
			nil,
		}[0],
	}
)

type resultOperatorNew struct {
	op    *Operator
	error *Error
}

type Operator struct {
	ptr uintptr
}

func (o *Operator) Write(path string, data []byte) error {
	return operatorWrite(o, path, data)
}

func (o *Operator) Read(path string) ([]byte, error) {
	return operatorRead(o, path)
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

type OperatorOptions struct {
	inner uintptr
}

func (opts *OperatorOptions) Set(key, value string) error {
	return operatorOptionsSet(opts, key, value)
}

type resultRead struct {
	data  *openDALBytes
	error *Error
}

type openDALBytes struct {
	data []byte
}

var libopendal uintptr

func init() {
	var err error
	libopendal, err = purego.Dlopen("./libopendal_c.so", purego.RTLD_LAZY)
	if err != nil {
		panic(err)
	}
}

func NewOptions() (*OperatorOptions, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 0, &ffi.TypePointer); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_new")
	if err != nil {
		return nil, err
	}
	fn := func() OperatorOptions {
		var opts OperatorOptions
		ffi.Call(&cif, sym, unsafe.Pointer(&opts))
		return opts
	}
	opts := fn()
	runtime.SetFinalizer(&opts, func(_ *OperatorOptions) {
		operatorOptionsFree(&opts)
	})
	return &opts, nil
}

func operatorOptionsSet(opts *OperatorOptions, key, value string) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 3, &ffi.TypeVoid, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_set")
	if err != nil {
		return err
	}
	fn := func(opts *OperatorOptions, key, value string) error {
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

func operatorOptionsFree(opts *OperatorOptions) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 1, &ffi.TypeVoid, &ffi.TypePointer); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_options_free")
	if err != nil {
		return err
	}
	fn := func(opts *OperatorOptions) {
		ffi.Call(&cif, sym, nil, unsafe.Pointer(&opts))
	}
	fn(opts)
	return nil
}

func NewOperator(name string, opts *OperatorOptions) (*Operator, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 2, &TypeResultOperatorNew, &ffi.TypePointer, &ffi.TypePointer); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_new")
	if err != nil {
		return nil, err
	}
	fn := func(name string, opts *OperatorOptions) (*resultOperatorNew, error) {
		byteName, err := unix.BytePtrFromString(name)
		if err != nil {
			return nil, err
		}
		var result resultOperatorNew
		ffi.Call(&cif, sym, unsafe.Pointer(&result), unsafe.Pointer(&byteName), unsafe.Pointer(opts))
		return &result, nil
	}
	result, err := fn(name, opts)
	if err != nil {
		return nil, err
	}
	if result.error != nil {
		return nil, result.error
	}
	return result.op, nil
}

func operatorWrite(op *Operator, path string, data []byte) error {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 3, &TypeOpenDALError, &ffi.TypePointer, &ffi.TypePointer, &TypeBytes); status != ffi.OK {
		return errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_write")
	if err != nil {
		return err
	}
	fn := func(op *Operator, path string, data []byte) error {
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

	return fn(op, path, data)
}

func operatorRead(op *Operator, path string) ([]byte, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 2, &TypeResultRead, &ffi.TypePointer, &ffi.TypePointer); status != ffi.OK {
		panic(status)
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_read")
	fn := func(op *Operator, path string) (*resultRead, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var r resultRead
		ffi.Call(&cif, sym, unsafe.Pointer(&r), unsafe.Pointer(&op), unsafe.Pointer(&bytePath))
		return &r, nil
	}
	result, err := fn(op, path)
	if err != nil {
		return nil, err
	}
	if result.error != nil {
		return nil, result.error
	}
	return result.data.data, nil
}
