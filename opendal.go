package opendal

import (
	"runtime"

	"github.com/ebitengine/purego"
)

type OperatorOptions map[string]string

type Operator struct {
	inner *operator

	write operatorWrite

	read operatorRead
}

func NewOperator(scheme Schemer, opts OperatorOptions) (op *Operator, err error) {
	libopendal, err := purego.Dlopen(scheme.Path(), purego.RTLD_LAZY|purego.RTLD_GLOBAL)
	if err != nil {
		return
	}
	opt, err := newOperatorOptions(libopendal)
	if err != nil {
		purego.Dlclose(libopendal)
		return
	}
	for key, value := range opts {
		operatorOptionsSet(libopendal, &opt, key, value)
	}

	inner, err := newOperator(libopendal, scheme, &opt)
	if err != nil {
		operatorOptionsFree(libopendal, &opt)
		purego.Dlclose(libopendal)
		return
	}

	defer operatorOptionsFree(libopendal, &opt)

	op = &Operator{
		inner: inner,
	}

	runtime.SetFinalizer(op, func(_ *Operator) {
		purego.Dlclose(libopendal)
	})

	for _, register := range operatorRegisters {
		err = register(libopendal, op)
		if err != nil {
			return
		}
	}

	return
}
