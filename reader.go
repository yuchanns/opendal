package opendal

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (op *Operator) Read(path string) ([]byte, error) {
	read := getFFI[operatorRead](op.ctx, symOperatorRead)
	bytes, err := read(op.inner, path)
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if len(data) > 0 {
		free := getFFI[bytesFree](op.ctx, symBytesFree)
		free(bytes)

	}
	return data, nil
}

func (op *Operator) Reader(path string) (*OperatorReader, error) {
	getReader := getFFI[operatorReader](op.ctx, symOperatorReader)
	inner, err := getReader(op.inner, path)
	if err != nil {
		return nil, err
	}
	reader := &OperatorReader{
		inner: inner,
		op:    op,
	}
	runtime.SetFinalizer(reader, func(_ *OperatorReader) {
		free := getFFI[readerFree](op.ctx, symReaderFree)
		free(inner)
	})
	return reader, nil
}

type OperatorReader struct {
	inner *opendalReader
	op    *Operator // // hold the op pointer to ensure it is gc after OperatorReader instance.
}

func (r *OperatorReader) Read(length uint) ([]byte, error) {
	read := getFFI[readerRead](r.op.ctx, symReaderRead)
	buf := make([]byte, length)
	var (
		totalSize uint
		size      uint
		err       error
	)
	for {
		size, err = read(r.inner, buf[totalSize:])
		totalSize += size
		if size == 0 || err != nil || totalSize >= length {
			break
		}
	}
	return buf[:totalSize], err
}

const symOperatorRead = "opendal_operator_read"

type operatorRead func(op *opendalOperator, path string) (*opendalBytes, error)

var withOperatorRead = withFFI(ffiOpts{
	sym:    symOperatorRead,
	rType:  &typeResultRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorRead {
	return func(op *opendalOperator, path string) (*opendalBytes, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return result.data, parseError(ctx, result.error)
	}
})

const symOperatorReader = "opendal_operator_reader"

type operatorReader func(op *opendalOperator, path string) (*opendalReader, error)

var withOperatorReader = withFFI(ffiOpts{
	sym:    symOperatorReader,
	rType:  &typeResultOperatorReader,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorReader {
	return func(op *opendalOperator, path string) (*opendalReader, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultOperatorReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

const symReaderFree = "opendal_reader_free"

type readerFree func(r *opendalReader)

var withReaderFree = withFFI(ffiOpts{
	sym:    symReaderFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) readerFree {
	return func(r *opendalReader) {
		ffiCall(
			nil,
			unsafe.Pointer(&r),
		)
	}
})

const symReaderRead = "opendal_reader_read"

type readerRead func(r *opendalReader, buf []byte) (size uint, err error)

var withReaderRead = withFFI(ffiOpts{
	sym:    symReaderRead,
	rType:  &typeResultReaderRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) readerRead {
	return func(r *opendalReader, buf []byte) (size uint, err error) {
		var length = len(buf)
		if length == 0 {
			return 0, nil
		}
		bytePtr := &buf[0]
		var result resultReaderRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&r),
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&length),
		)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return result.size, nil
	}
})
