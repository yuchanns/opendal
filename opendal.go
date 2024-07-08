package opendal

import (
	"context"
	"runtime"

	"github.com/ebitengine/purego"
)

type OperatorOptions map[string]string

type Operator struct {
	ctx context.Context

	inner *opendalOperator
}

func NewOperator(scheme Schemer, opts OperatorOptions) (op *Operator, err error) {
	libopendal, err := purego.Dlopen(scheme.Path(), purego.RTLD_LAZY|purego.RTLD_GLOBAL)
	if err != nil {
		return
	}
	ctx, err := registerCFn(libopendal)
	if err != nil {
		return
	}
	opt, err := newOperatorOptions(libopendal)
	if err != nil {
		purego.Dlclose(libopendal)
		return
	}
	setOptions := getCFn[operatorOptionsSet](ctx, cFnOperatorOptionsSet)
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

func registerCFn(libopendal uintptr) (ctx context.Context, err error) {
	ctx = context.Background()
	for _, register := range cFnRegisters {
		ctx, err = register(ctx, libopendal)
		if err != nil {
			return
		}
	}
	return
}

type cFnRegister func(context.Context, uintptr) (context.Context, error)

func getCFn[T any](ctx context.Context, key string) T {
	return ctx.Value(key).(T)
}

var cFnRegisters = []cFnRegister{
	// two registers must be on top
	bytesFreeRegister,
	errorFreeRegister,

	operatorOptionsSetRegister,

	operatorCreateDirRegister,
	operatorReadRegister,
	operatorWriteRegister,
	operatorDeleteRegister,
	operatorStatRegister,

	metadataContentLengthRegister,
	metadataIsFileRegister,
	metadataIsDirRegister,
	metadataLastModifiedMsRegister,
	metadataFreeRegister,
}
