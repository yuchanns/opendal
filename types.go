package opendal

import (
	"fmt"

	"github.com/jupiterrider/ffi"
)

func newTypePointer(elems **ffi.Type) ffi.Type {
	return ffi.Type{
		Type:     ffi.Pointer,
		Elements: elems,
	}
}

var (
	typeError = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypeSint32,
			&typeBytes,
			nil,
		}[0],
	}

	typeResultOperatorNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeResultRead = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeBytes = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypeUint64,
			nil,
		}[0],
	}

	typeResultStat = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&typeMetadataPointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeMetadata = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeMetadataPointer = newTypePointer(
		&[]*ffi.Type{
			&typeMetadata,
			nil,
		}[0],
	)
)

type resultOperatorNew struct {
	op    *operator
	error *Error
}

type operator struct {
	ptr uintptr
}

type resultRead struct {
	data  *bytes
	error *Error
}

type resultStat struct {
	meta  *meta
	error *Error
}

type meta struct {
	inner uintptr
}

type bytes struct {
	data []byte
}

type Error struct {
	code    int32
	message bytes
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d %s", e.code, e.message.data)
}

func (e *Error) Code() int32 {
	return e.code
}

func (e *Error) Message() string {
	return string(e.message.data)
}
