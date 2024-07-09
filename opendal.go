package opendal

import (
	"context"
	"runtime"

	"github.com/ebitengine/purego"
)

type Schemer interface {
	Scheme() string
	Path() (string, error)
}

type OperatorOptions map[string]string

type Operator struct {
	ctx context.Context

	inner *opendalOperator
}

func NewOperator(scheme Schemer, opts OperatorOptions) (op *Operator, err error) {
	path, err := scheme.Path()
	if err != nil {
		return
	}
	libopendal, err := purego.Dlopen(path, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
	if err != nil {
		return
	}
	ctx, err := contextWithCFuncs(libopendal)
	if err != nil {
		return
	}
	opt, err := newOperatorOptions(libopendal)
	if err != nil {
		purego.Dlclose(libopendal)
		return
	}
	setOptions := getCFunc[operatorOptionsSet](ctx, symOperatorOptionSet)
	for key, value := range opts {
		setOptions(opt, key, value)
	}

	inner, err := newOperator(ctx, libopendal, scheme, opt)
	if err != nil {
		operatorOptionsFree(libopendal, opt)
		purego.Dlclose(libopendal)
		return
	}

	defer operatorOptionsFree(libopendal, opt)

	op = &Operator{
		inner: inner,
		ctx:   ctx,
	}

	runtime.SetFinalizer(op, func(_ *Operator) {
		operatorFree(libopendal, inner)
		purego.Dlclose(libopendal)
	})

	return
}

func contextWithCFuncs(libopendal uintptr) (ctx context.Context, err error) {
	ctx = context.Background()
	for _, register := range withCFuncs {
		ctx, err = register(ctx, libopendal)
		if err != nil {
			return
		}
	}
	return
}

type withCFunc func(context.Context, uintptr) (context.Context, error)

func getCFunc[T any](ctx context.Context, key string) T {
	return ctx.Value(key).(T)
}

var withCFuncs = []withCFunc{
	// free must be on top
	withBytesFree,
	withErrorFree,

	withOperatorOptionsSet,

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
