package opendal

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

// ErrorCode is all kinds of ErrorCode of opendal
type ErrorCode int32

const (
	// OpenDAL don't know what happend here, and no actions other than just
	// returning it back. For example, s3 returns an internal service error.
	CodeUnexpected ErrorCode = iota
	// Underlying service doesn't support this operation.
	CodeUnsupported
	// The config for backend is invalid.
	CodeConfigInvalid
	// The given path is not found.
	CodeNotFound
	// The given path doesn't have enough permission for this operation
	CodePermissioDenied
	// The given path is a directory.
	CodeIsADirectory
	// The given path is not a directory.
	CodeNotADirectory
	// The given path already exists thus we failed to the specified operation on it.
	CodeAlreadyExists
	// Requests that sent to this path is over the limit, please slow down.
	CodeRateLimited
	// The given file paths are same.
	CodeIsSameFile
	// The condition of this operation is not match.
	//
	// The `condition` itself is context based.
	//
	// For example, in S3, the `condition` can be:
	// 1. writing a file with If-Match header but the file's ETag is not match (will get a 412 Precondition Failed).
	// 2. reading a file with If-None-Match header but the file's ETag is match (will get a 304 Not Modified).
	//
	// As OpenDAL cannot handle the `condition not match` error, it will always return this error to users.
	// So users could to handle this error by themselves.
	CodeConditionNotMatch
	// The range of the content is not satisfied.
	//
	// OpenDAL returns this error to indicate that the range of the read request is not satisfied.
	CodeRangeNotSatisfied
)

func parseError(ctx context.Context, err *opendalError) error {
	if err == nil {
		return nil
	}
	free := getCFunc[errorFree](ctx, symErrorFree)
	defer free(err)
	return &Error{
		code:    ErrorCode(err.code),
		message: string(parseBytes(&err.message)),
	}
}

type Error struct {
	code    ErrorCode
	message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d %s", e.code, e.message)
}

func (e *Error) Code() ErrorCode {
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
