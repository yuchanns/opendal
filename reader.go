package opendal

import (
	"context"
	"errors"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

func (o *Operator) Read(path string) ([]byte, error) {
	read := getCFunc[operatorRead](o.ctx, symOperatorRead)
	bytes, err := read(o.inner, path)
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if len(data) > 0 {
		free := getCFunc[bytesFree](o.ctx, symBytesFree)
		free(bytes)

	}
	return data, nil
}

func (o *Operator) Reader(path string) (*OperatorReader, error) {
	getReader := getCFunc[operatorReader](o.ctx, symOperatorReader)
	inner, err := getReader(o.inner, path)
	if err != nil {
		return nil, err
	}
	reader := &OperatorReader{
		inner: inner,
		ctx:   o.ctx,
	}
	runtime.SetFinalizer(reader, func(_ *OperatorReader) {
		free := getCFunc[readerFree](o.ctx, symReaderFree)
		free(inner)
	})
	return reader, nil
}

type OperatorReader struct {
	inner *opendalReader
	ctx   context.Context
}

func (r *OperatorReader) Read(length uint) ([]byte, uint, error) {
	read := getCFunc[readerRead](r.ctx, symReaderRead)
	buf := make([]byte, length)
	size, err := read(r.inner, buf)
	return buf, size, err
}

const symOperatorRead = "opendal_operator_read"

type operatorRead func(op *opendalOperator, path string) (*opendalBytes, error)

func withOperatorRead(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
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
	fn, err := purego.Dlsym(libopendal, symOperatorRead)
	if err != nil {
		return
	}
	var cFn operatorRead = func(op *opendalOperator, path string) (*opendalBytes, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultRead
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return result.data, parseError(ctx, result.error)
	}
	newCtx = context.WithValue(ctx, symOperatorRead, cFn)
	return
}

const symOperatorReader = "opendal_operator_reader"

type operatorReader func(op *opendalOperator, path string) (*opendalReader, error)

func withOperatorReader(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 2,
		&typeResultOperatorReader,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symOperatorReader)
	if err != nil {
		return
	}
	var cFn operatorReader = func(op *opendalOperator, path string) (*opendalReader, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultOperatorReader
		ffi.Call(
			&cif, fn,
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
	newCtx = context.WithValue(ctx, symOperatorReader, cFn)
	return
}

const symReaderFree = "opendal_reader_free"

type readerFree func(r *opendalReader)

func withReaderFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symReaderFree)
	if err != nil {
		return
	}
	var cFn readerFree = func(r *opendalReader) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&r),
		)
	}
	newCtx = context.WithValue(ctx, symReaderFree, cFn)
	return
}

const symReaderRead = "opendal_reader_read"

type readerRead func(r *opendalReader, buf []byte) (size uint, err error)

func withReaderRead(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 3,
		&typeResultReaderRead,
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symReaderRead)
	if err != nil {
		return
	}
	var cFn readerRead = func(r *opendalReader, buf []byte) (size uint, err error) {
		var length = len(buf)
		if length == 0 {
			return 0, nil
		}
		bytePtr := &buf[0]
		var result resultReaderRead
		ffi.Call(&cif, fn, unsafe.Pointer(&result), unsafe.Pointer(&r), unsafe.Pointer(&bytePtr), unsafe.Pointer(&length))
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return uint(len(buf)), nil
	}
	newCtx = context.WithValue(ctx, symReaderRead, cFn)
	return
}
