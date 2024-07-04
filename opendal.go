package opendal

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

type Operator struct {
	inner      *RawOperator
	libopendal uintptr
}

func (o *Operator) Write(path string, data []byte) error {
	return operatorWrite(o, path, data)
}

func (o *Operator) Read(path string) ([]byte, error) {
	return operatorRead(o, path)
}

func NewOperator(scheme Schemer, opts Options) (*Operator, error) {
	libopendal, err := purego.Dlopen(scheme.Path(), purego.RTLD_LAZY)
	if err != nil {
		return nil, err
	}
	var cif ffi.Cif
	if status := ffi.PrepCif(&cif, ffi.DefaultAbi, 2, &typeResultOperatorNew, &ffi.TypePointer, &ffi.TypePointer); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	sym, err := purego.Dlsym(libopendal, "opendal_operator_new")
	if err != nil {
		return nil, err
	}
	fn := func(scheme Schemer, opts *operatorOptions) (*resultOperatorNew, error) {
		byteName, err := unix.BytePtrFromString(scheme.Scheme())
		if err != nil {
			return nil, err
		}
		var result resultOperatorNew
		ffi.Call(&cif, sym, unsafe.Pointer(&result), unsafe.Pointer(&byteName), unsafe.Pointer(opts))
		return &result, nil
	}
	rawOpts, err := newRawOptions(libopendal)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		operatorOptionsSet(libopendal, rawOpts, opt.key, opt.value)
	}
	defer operatorOptionsFree(libopendal, *rawOpts)
	result, err := fn(scheme, rawOpts)
	if err != nil {
		return nil, err
	}
	if result.error != nil {
		return nil, result.error
	}
	op := &Operator{
		inner:      result.op,
		libopendal: libopendal,
	}
	return op, nil
}

type Options []struct {
	key   string
	value string
}

func NewOptions() Options {
	return Options{}
}

func (o *Options) Set(key, value string) {
	*o = append(*o, struct {
		key   string
		value string
	}{
		key,
		value,
	})
}
