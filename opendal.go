package opendal

import (
	"context"
	"runtime"
)

type Scheme interface {
	Name() string
	Path() string
	LoadOnce() error
}

type OperatorOptions map[string]string

type Operator struct {
	ctx context.Context

	inner *opendalOperator
}

func NewOperator(scheme Scheme, opts OperatorOptions) (op *Operator, err error) {
	err = scheme.LoadOnce()
	if err != nil {
		return
	}

	ctx, cancel, err := contextWithFFIs(scheme.Path())
	if err != nil {
		return
	}

	options := getFFI[operatorOptionsNew](ctx, symOperatorOptionsNew)()
	setOptions := getFFI[operatorOptionsSet](ctx, symOperatorOptionSet)
	optionsFree := getFFI[operatorOptionsFree](ctx, symOperatorOptionsFree)

	for key, value := range opts {
		setOptions(options, key, value)
	}

	inner, err := getFFI[operatorNew](ctx, symOperatorNew)(scheme, options)
	if err != nil {
		optionsFree(options)
		cancel()
		return
	}

	defer optionsFree(options)

	op = &Operator{
		inner: inner,
		ctx:   ctx,
	}

	runtime.SetFinalizer(op, func(_ *Operator) {
		getFFI[operatorFree](ctx, symOperatorFree)(inner)
		cancel()
	})

	return
}
