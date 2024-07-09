package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Copy(src, dest string) error {
	cp := getCFunc[operatorCopy](op.ctx, symOperatorCopy)
	return cp(op.inner, src, dest)
}

func (op *Operator) Rename(src, dest string) error {
	rename := getCFunc[operatorRename](op.ctx, symOperatorRename)
	return rename(op.inner, src, dest)
}

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
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorOptionSet,
		nArgs:  3,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorOptionsSet {
		return func(opts *operatorOptions, key, value string) error {
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
			ffi.Call(cif, fn, nil, unsafe.Pointer(&opts), unsafe.Pointer(&byteKey), unsafe.Pointer(&byteValue))
			return nil
		}
	})
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

const symOperatorCopy = "opendal_operator_copy"

type operatorCopy func(op *opendalOperator, src, dest string) (err error)

func withOperatorCopy(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorCopy,
		nArgs:  3,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorCopy {
		return func(op *opendalOperator, src, dest string) error {
			var (
				byteSrc  *byte
				byteDest *byte
			)
			byteSrc, err = unix.BytePtrFromString(src)
			if err != nil {
				return err
			}
			byteDest, err = unix.BytePtrFromString(dest)
			if err != nil {
				return err
			}
			var err *opendalError
			ffi.Call(cif, fn, unsafe.Pointer(&err), unsafe.Pointer(&op), unsafe.Pointer(&byteSrc), unsafe.Pointer(&byteDest))
			return parseError(ctx, err)
		}
	})
}

const symOperatorRename = "opendal_operator_rename"

type operatorRename func(op *opendalOperator, src, dest string) (err error)

func withOperatorRename(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorRename,
		nArgs:  3,
		rType:  &ffi.TypePointer,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorRename {
		return func(op *opendalOperator, src, dest string) error {
			var (
				byteSrc  *byte
				byteDest *byte
			)
			byteSrc, err = unix.BytePtrFromString(src)
			if err != nil {
				return err
			}
			byteDest, err = unix.BytePtrFromString(dest)
			if err != nil {
				return err
			}
			var err *opendalError
			ffi.Call(cif, fn, unsafe.Pointer(&err), unsafe.Pointer(&op), unsafe.Pointer(&byteSrc), unsafe.Pointer(&byteDest))
			return parseError(ctx, err)
		}
	})
}

const symBytesFree = "opendal_bytes_free"

type bytesFree func(b *opendalBytes)

func withBytesFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symBytesFree,
		nArgs:  1,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) bytesFree {
		return func(b *opendalBytes) {
			ffi.Call(
				cif, fn,
				nil,
				unsafe.Pointer(&b),
			)
		}
	})
}
