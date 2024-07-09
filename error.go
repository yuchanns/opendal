package opendal

import (
	"context"
	"fmt"
	"unsafe"

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
	free := getFFI[errorFree](ctx, symErrorFree)
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
	return withFFI(ctx, libopendal, ffiOpts{
		sym:    symErrorFree,
		nArgs:  1,
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer},
	}, func(cif *ffi.Cif, fn uintptr) errorFree {
		return func(e *opendalError) {
			ffi.Call(
				cif, fn,
				nil,
				unsafe.Pointer(&e),
			)
		}
	})
}
