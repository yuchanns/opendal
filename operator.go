package opendal

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Copy(src, dest string) error {
	cp := getFFI[operatorCopy](op.ctx, symOperatorCopy)
	return cp(op.inner, src, dest)
}

func (op *Operator) Rename(src, dest string) error {
	rename := getFFI[operatorRename](op.ctx, symOperatorRename)
	return rename(op.inner, src, dest)
}

const symOperatorNew = "opendal_operator_new"

type operatorNew func(scheme Schemer, opts *operatorOptions) (op *opendalOperator, err error)

func withOperatorNew(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorNew,
		nArgs:  2,
		rType:  &typeResultOperatorNew,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorNew {
		return func(scheme Schemer, opts *operatorOptions) (op *opendalOperator, err error) {
			var byteName *byte
			byteName, err = unix.BytePtrFromString(scheme.Scheme())
			if err != nil {
				return
			}
			var result resultOperatorNew
			ffi.Call(cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&byteName), unsafe.Pointer(&opts))
			if result.error != nil {
				err = parseError(ctx, result.error)
				return
			}
			op = result.op
			return
		}
	})
}

const symOperatorFree = "opendal_operator_free"

type operatorFree func(op *opendalOperator)

func withOperatorFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorFree,
		nArgs:  1,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorFree {
		return func(op *opendalOperator) {
			ffi.Call(cif, fn, nil, unsafe.Pointer(&op))
		}
	})
}

type operatorOptions struct {
	inner uintptr
}

const symOperatorOptionsNew = "opendal_operator_options_new"

type operatorOptionsNew func() (opts *operatorOptions)

func withOperatorOptionsNew(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:   symOperatorOptionsNew,
		nArgs: 0,
		rType: &ffi.TypePointer,
	}, func(cif *ffi.Cif, fn uintptr) operatorOptionsNew {
		return func() (opts *operatorOptions) {
			ffi.Call(cif, fn, unsafe.Pointer(&opts))
			return
		}
	})
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

const symOperatorOptionsFree = "opendal_operator_options_free"

type operatorOptionsFree func(opts *operatorOptions)

func withOperatorOptionsFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorOptionsFree,
		nArgs:  1,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorOptionsFree {
		return func(opts *operatorOptions) {
			ffi.Call(cif, fn, nil, unsafe.Pointer(&opts))
		}
	})
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
