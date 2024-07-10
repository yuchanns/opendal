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

var withOperatorNew = withFFI(ffiOpts{
	sym:    symOperatorNew,
	nArgs:  2,
	rType:  &typeResultOperatorNew,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorNew {
	return func(scheme Schemer, opts *operatorOptions) (op *opendalOperator, err error) {
		var byteName *byte
		byteName, err = unix.BytePtrFromString(scheme.Scheme())
		if err != nil {
			return
		}
		var result resultOperatorNew
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&byteName),
			unsafe.Pointer(&opts),
		)
		if result.error != nil {
			err = parseError(ctx, result.error)
			return
		}
		op = result.op
		return
	}
})

const symOperatorFree = "opendal_operator_free"

type operatorFree func(op *opendalOperator)

var withOperatorFree = withFFI(ffiOpts{
	sym:    symOperatorFree,
	nArgs:  1,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorFree {
	return func(op *opendalOperator) {
		ffiCall(
			nil,
			unsafe.Pointer(&op),
		)
	}
})

type operatorOptions struct {
	inner uintptr
}

const symOperatorOptionsNew = "opendal_operator_options_new"

type operatorOptionsNew func() (opts *operatorOptions)

var withOperatorOptionsNew = withFFI(ffiOpts{
	sym:   symOperatorOptionsNew,
	nArgs: 0,
	rType: &ffi.TypePointer,
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorOptionsNew {
	return func() (opts *operatorOptions) {
		ffiCall(unsafe.Pointer(&opts))
		return
	}
})

const symOperatorOptionSet = "opendal_operator_options_set"

type operatorOptionsSet func(opts *operatorOptions, key, value string) error

var withOperatorOptionsSet = withFFI(ffiOpts{
	sym:    symOperatorOptionSet,
	nArgs:  3,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorOptionsSet {
	return func(opts *operatorOptions, key, value string) (err error) {
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
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
			unsafe.Pointer(&byteKey),
			unsafe.Pointer(&byteValue),
		)
		return nil
	}
})

const symOperatorOptionsFree = "opendal_operator_options_free"

type operatorOptionsFree func(opts *operatorOptions)

var withOperatorOptionsFree = withFFI(ffiOpts{
	sym:    symOperatorOptionsFree,
	nArgs:  1,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorOptionsFree {
	return func(opts *operatorOptions) {
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
		)
	}
})

const symOperatorCopy = "opendal_operator_copy"

type operatorCopy func(op *opendalOperator, src, dest string) (err error)

var withOperatorCopy = withFFI(ffiOpts{
	sym:    symOperatorCopy,
	nArgs:  3,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorCopy {
	return func(op *opendalOperator, src, dest string) (err error) {
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
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&byteSrc),
			unsafe.Pointer(&byteDest),
		)
		return parseError(ctx, e)
	}
})

const symOperatorRename = "opendal_operator_rename"

type operatorRename func(op *opendalOperator, src, dest string) (err error)

var withOperatorRename = withFFI(ffiOpts{
	sym:    symOperatorRename,
	nArgs:  3,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorRename {
	return func(op *opendalOperator, src, dest string) (err error) {
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
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&err),
			unsafe.Pointer(&op),
			unsafe.Pointer(&byteSrc),
			unsafe.Pointer(&byteDest),
		)
		return parseError(ctx, e)
	}
})

const symBytesFree = "opendal_bytes_free"

type bytesFree func(b *opendalBytes)

var withBytesFree = withFFI(ffiOpts{
	sym:    symBytesFree,
	nArgs:  1,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) bytesFree {
	return func(b *opendalBytes) {
		ffiCall(
			nil,
			unsafe.Pointer(&b),
		)
	}
})
