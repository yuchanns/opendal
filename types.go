package opendal

import (
	"unsafe"

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
			&ffi.TypePointer,
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
	error *opendalError
}

type operator struct {
	ptr uintptr
}

type resultRead struct {
	data  *opendalBytes
	error *opendalError
}

type resultStat struct {
	meta  *opendalMetadata
	error *opendalError
}

type opendalMetadata struct {
	inner uintptr
}

type opendalBytes struct {
	data *byte
	len  uintptr
}

func toOpendalBytes(data []byte) opendalBytes {
	var ptr *byte
	l := len(data)
	if l > 0 {
		ptr = &data[0]
	}
	return opendalBytes{
		data: ptr,
		len:  uintptr(l),
	}
}

func (b *opendalBytes) toByteSlice() []byte {
	return unsafe.Slice(b.data, b.len)[:]
}

type opendalError struct {
	code    int32
	message opendalBytes
}
