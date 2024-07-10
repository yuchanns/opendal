package opendal

import (
	"context"
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
)

func contextWithFFIs(path string) (ctx context.Context, cancel context.CancelFunc, err error) {
	libopendal, err := purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
	if err != nil {
		return
	}
	ctx = context.Background()
	for _, withFFI := range withFFIs {
		ctx, err = withFFI(ctx, libopendal)
		if err != nil {
			return
		}
	}
	ctx, cancel = context.WithCancel(ctx)
	cancel = func() {
		purego.Dlclose(libopendal)
		cancel()
	}
	return
}

type contextWithFFI func(ctx context.Context, libopendal uintptr) (context.Context, error)

func getFFI[T any](ctx context.Context, key string) T {
	return ctx.Value(key).(T)
}

type ffiOpts struct {
	sym    string
	nArgs  uint32
	rType  *ffi.Type
	aTypes []*ffi.Type
}

func withFFI[T any](
	opts ffiOpts,
	withFunc func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) T,
) func(ctx context.Context, libopendal uintptr) (context.Context, error) {
	return func(ctx context.Context, libopendal uintptr) (context.Context, error) {
		var cif ffi.Cif
		if status := ffi.PrepCif(
			&cif, ffi.DefaultAbi, opts.nArgs,
			opts.rType,
			opts.aTypes...,
		); status != ffi.OK {
			return nil, errors.New(status.String())
		}
		fn, err := purego.Dlsym(libopendal, opts.sym)
		if err != nil {
			return nil, err
		}
		return context.WithValue(ctx, opts.sym,
			withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
				ffi.Call(&cif, fn, rValue, aValues...)
			}),
		), nil
	}
}

var withFFIs = []contextWithFFI{
	// free must be on top
	withBytesFree,
	withErrorFree,

	withOperatorOptionsNew,
	withOperatorOptionsSet,
	withOperatorOptionsFree,

	withOperatorNew,
	withOperatorFree,

	withOperatorInfoNew,
	withOperatorInfoGetFullCapability,
	withOperatorInfoGetNativeCapability,
	withOperatorInfoGetScheme,
	withOperatorInfoGetRoot,
	withOperatorInfoGetName,
	withOperatorInfoFree,

	withOperatorCreateDir,
	withOperatorRead,
	withOperatorWrite,
	withOperatorDelete,
	withOperatorStat,
	withOperatorIsExists,
	withOperatorCopy,
	withOperatorRename,

	withMetaContentLength,
	withMetaIsFile,
	withMetaIsDir,
	withMetaLastModified,
	withMetaFree,

	withOperatorList,
	withListerNext,
	withListerFree,
	withEntryName,
	withEntryPath,
	withEntryFree,

	withOperatorReader,
	withReaderRead,
	withReaderFree,
}
