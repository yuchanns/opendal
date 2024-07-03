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

type ResultOperatorNew struct {
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
	message OpenDALBytes
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

type ResultRead struct {
	data  *OpenDALBytes
	error *Error
}

type OpenDALBytes struct {
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
	return &opts, nil
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
	fn := func(name string, opts OperatorOptions) (*ResultOperatorNew, error) {
		byteName, err := unix.BytePtrFromString(name)
		if err != nil {
			return nil, err
		}
		var result ResultOperatorNew
		ffi.Call(&cif, sym, unsafe.Pointer(&result), unsafe.Pointer(&byteName), unsafe.Pointer(&opts))
		return &result, nil
	}
	result, err := fn(name, *opts)
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
		bytes := OpenDALBytes{
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
	fn := func(op *Operator, path string) (*ResultRead, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var r ResultRead
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
