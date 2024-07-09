package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

var (
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

	typeResultList = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeResultListerNext = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	typeCapability = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypeUint32, // stat
			&ffi.TypeUint32, // stat_with_if_match
			&ffi.TypeUint32, // stat_with_if_none_match
			&ffi.TypeUint32, // read
			&ffi.TypeUint32, // read_with_if_match
			&ffi.TypeUint32, // read_with_if_match_none
			&ffi.TypeUint32, // read_with_override_cache_control
			&ffi.TypeUint32, // read_with_override_content_disposition
			&ffi.TypeUint32, // read_with_override_content_type
			&ffi.TypeUint32, // write
			&ffi.TypeUint32, // write_can_multi
			&ffi.TypeUint32, // write_can_empty
			&ffi.TypeUint32, // write_can_append
			&ffi.TypeUint32, // write_with_content_type
			&ffi.TypeUint32, // write_with_content_disposition
			&ffi.TypeUint32, // write_with_cache_control
			&ffi.TypeUint32, // write_multi_max_size
			&ffi.TypeUint32, // write_multi_min_size
			&ffi.TypeUint32, // write_multi_align_size
			&ffi.TypeUint32, // write_total_max_size
			&ffi.TypeUint32, // create_dir
			&ffi.TypeUint32, // delete
			&ffi.TypeUint32, // copy
			&ffi.TypeUint32, // rename
			&ffi.TypeUint32, // list
			&ffi.TypeUint32, // list_with_limit
			&ffi.TypeUint32, // list_with_start_after
			&ffi.TypeUint32, // list_with_recursive
			&ffi.TypeUint32, // presign
			&ffi.TypeUint32, // presign_read
			&ffi.TypeUint32, // presign_stat
			&ffi.TypeUint32, // presign_write
			&ffi.TypeUint32, // batch
			&ffi.TypeUint32, // batch_delete
			&ffi.TypeUint32, // batch_max_operations
			&ffi.TypeUint32, // blocking
			nil,
		}[0],
	}
)

type opendalCapability struct {
	stat                               uint8
	statWithIfmatch                    uint8
	statWithIfNoneMatch                uint8
	read                               uint8
	readWithIfmatch                    uint8
	readWithIfMatchNone                uint8
	readWithOverrideCacheControl       uint8
	readWithOverrideContentDisposition uint8
	readWithOverrideContentType        uint8
	write                              uint8
	writeCanMulti                      uint8
	writeCanEmpty                      uint8
	writeCanAppend                     uint8
	writeWithContentType               uint8
	writeWithContentDisposition        uint8
	writeWithCacheControl              uint8
	writeMultiMaxSize                  uint
	writeMultiMinSize                  uint
	writeMultiAlignSize                uint
	writeTotalMaxSize                  uint
	createDir                          uint8
	delete                             uint8
	copy                               uint8
	rename                             uint8
	list                               uint8
	listWithLimit                      uint8
	listWithStartAfter                 uint8
	listWithRecursive                  uint8
	presign                            uint8
	presignRead                        uint8
	presignStat                        uint8
	presignWrite                       uint8
	batch                              uint8
	batchDelete                        uint8
	batchMaxOperations                 uint
	blocking                           uint8
}

type resultOperatorNew struct {
	op    *opendalOperator
	error *opendalError
}

type opendalOperator struct {
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

type opendalError struct {
	code    int32
	message opendalBytes
}

type opendalOperatorInfo struct {
	inner uintptr
}

type opendalResultList struct {
	lister *opendalLister
	err    *opendalError
}

type opendalLister struct {
	inner uintptr
}

type opendalResultListerNext struct {
	entry *opendalEntry
	err   *opendalError
}

type opendalEntry struct {
	inner uintptr
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

func parseBytes(b *opendalBytes) (data []byte) {
	if b == nil || b.len == 0 {
		return nil
	}
	data = make([]byte, b.len)
	copy(data, unsafe.Slice(b.data, b.len))
	return
}

func parseBytesWithFree(ctx context.Context, b *opendalBytes) (data []byte) {
	data = parseBytes(b)
	free := getCFunc[bytesFree](ctx, symBytesFree)
	free(b)
	return
}

type bytesFree func(b *opendalBytes)

const symBytesFree = "opendal_bytes_free"

func withBytesFree(ctx context.Context, libopendal uintptr) (newCtx context.Context, err error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif, ffi.DefaultAbi, 1,
		&ffi.TypeVoid,
		&ffi.TypePointer,
	); status != ffi.OK {
		err = errors.New(status.String())
		return
	}
	fn, err := purego.Dlsym(libopendal, symBytesFree)
	if err != nil {
		return
	}
	var cFn bytesFree = func(b *opendalBytes) {
		ffi.Call(
			&cif, fn,
			nil,
			unsafe.Pointer(&b),
		)
	}
	newCtx = context.WithValue(ctx, symBytesFree, cFn)
	return
}
