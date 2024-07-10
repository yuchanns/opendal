package opendal

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Read(path string) ([]byte, error) {
	read := getFFI[operatorRead](o.ctx, symOperatorRead)
	bytes, err := read(o.inner, path)
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if len(data) > 0 {
		free := getFFI[bytesFree](o.ctx, symBytesFree)
		free(bytes)

	}
	return data, nil
}

func (o *Operator) Reader(path string) (*OperatorReader, error) {
	getReader := getFFI[operatorReader](o.ctx, symOperatorReader)
	inner, err := getReader(o.inner, path)
	if err != nil {
		return nil, err
	}
	reader := &OperatorReader{
		inner: inner,
		ctx:   o.ctx,
	}
	runtime.SetFinalizer(reader, func(_ *OperatorReader) {
		free := getFFI[readerFree](o.ctx, symReaderFree)
		free(inner)
	})
	return reader, nil
}

type OperatorReader struct {
	inner *opendalReader
	ctx   context.Context
}

func (r *OperatorReader) Read(length uint) ([]byte, error) {
	read := getFFI[readerRead](r.ctx, symReaderRead)
	buf := make([]byte, length)
	size, err := read(r.inner, buf)
	return buf[:size], err
}

const symOperatorRead = "opendal_operator_read"

type operatorRead func(op *opendalOperator, path string) (*opendalBytes, error)

func withOperatorRead(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorRead,
		nArgs:  2,
		rType:  &typeResultRead,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorRead {
		return func(op *opendalOperator, path string) (*opendalBytes, error) {
			bytePath, err := unix.BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var result resultRead
			ffi.Call(
				cif, fn,
				unsafe.Pointer(&result),
				unsafe.Pointer(&op),
				unsafe.Pointer(&bytePath),
			)
			return result.data, parseError(ctx, result.error)
		}
	})
}

const symOperatorReader = "opendal_operator_reader"

type operatorReader func(op *opendalOperator, path string) (*opendalReader, error)

func withOperatorReader(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symOperatorReader,
		nArgs:  2,
		rType:  &typeResultOperatorReader,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) operatorReader {
		return func(op *opendalOperator, path string) (*opendalReader, error) {
			bytePath, err := unix.BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var result resultOperatorReader
			ffi.Call(
				cif, fn,
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
}

const symReaderFree = "opendal_reader_free"

type readerFree func(r *opendalReader)

func withReaderFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symReaderFree,
		nArgs:  1,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) readerFree {
		return func(r *opendalReader) {
			ffi.Call(
				cif, fn,
				nil,
				unsafe.Pointer(&r),
			)
		}
	})
}

const symReaderRead = "opendal_reader_read"

type readerRead func(r *opendalReader, buf []byte) (size uint, err error)

func withReaderRead(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symReaderRead,
		nArgs:  3,
		rType:  &typeResultReaderRead,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) readerRead {
		return func(r *opendalReader, buf []byte) (size uint, err error) {
			var length = len(buf)
			if length == 0 {
				return 0, nil
			}
			bytePtr := &buf[0]
			var result resultReaderRead
			ffi.Call(cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&r), unsafe.Pointer(&bytePtr), unsafe.Pointer(&length))
			if result.error != nil {
				return 0, parseError(ctx, result.error)
			}
			return result.size, nil
		}
	})
}
