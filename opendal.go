package opendal

import (
	"context"
	"runtime"

	"github.com/ebitengine/purego"
)

type OperatorOptions map[string]string

type Operator struct {
	ctx context.Context

	inner *operator
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
		setOptions(&opt, key, value)
	}

	inner, err := newOperator(ctx, libopendal, scheme, &opt)
	if err != nil {
		operatorOptionsFree(libopendal, &opt)
		purego.Dlclose(libopendal)
		return
	}

	defer operatorOptionsFree(libopendal, &opt)

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
