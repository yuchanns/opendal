package opendal

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Write(path string, data []byte) error {
	return o.write(o.inner, path, data)
}

type operatorWrite func(op *operator, path string, data []byte) error

func operatorWriteRegister(libopendal uintptr, op *Operator) (err error) {
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
	sym, err := purego.Dlsym(libopendal, "opendal_operator_write")
	if err != nil {
		return
	}
	op.write = func(op *operator, path string, data []byte) error {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return err
		}
		bytes := bytes{
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
	return
}
