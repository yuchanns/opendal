package opendal

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

func parseError(ctx context.Context, e *opendalError) error {
	if e == nil {
		return nil
	}
	free := getCFunc[errorFree](ctx, symErrorFree)
	defer free(e)
	return &Error{
		code:    e.code,
		message: string(parseBytes(&e.message)),
	}
}

type Error struct {
	code    int32
	message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d %s", e.code, e.message)
}

func (e *Error) Code() int32 {
	return e.code
}

func (e *Error) Message() string {
	return e.message
}

type errorFree func(e *opendalError)

const symErrorFree = "opendal_error_free"

func withErrorFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symErrorFree)
	if err != nil {
		return
	}
	var cFn errorFree = func(e *opendalError) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&e),
		)
	}
	newCtx = context.WithValue(ctx, symErrorFree, cFn)
	return
}
