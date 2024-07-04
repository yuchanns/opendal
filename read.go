package opendal

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Read(path string) ([]byte, error) {
	return o.read(o.inner, path)
}

type operatorRead func(op *operator, path string) ([]byte, error)

func operatorReadRegister(libopendal uintptr, op *Operator) (err error) {
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
	sym, err := purego.Dlsym(libopendal, "opendal_operator_read")
	op.read = func(op *operator, path string) ([]byte, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultRead
		ffi.Call(
			&cif, sym,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, result.error
		}
		return result.data.data, nil
	}
	return
}
