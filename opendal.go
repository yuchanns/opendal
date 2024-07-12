package opendal

import (
	"context"
)

type Scheme interface {
	Name() string
	Path() string
	LoadOnce() error
}

type OperatorOptions map[string]string

type Operator struct {
	ctx    context.Context
	cancel context.CancelFunc

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
	defer optionsFree(options)

	inner, err := getFFI[operatorNew](ctx, symOperatorNew)(scheme, options)
	if err != nil {
		cancel()
		return
	}

	op = &Operator{
		inner:  inner,
		ctx:    ctx,
		cancel: cancel,
	}

	return
}

func (op *Operator) Close() error {
	free := getFFI[operatorFree]
	free(op.ctx, symOperatorFree)(op.inner)
	op.cancel()

	return nil
}
