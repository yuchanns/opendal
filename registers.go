package opendal

import "context"

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
	bytesFreeRegister,
	errorFreeRegister,

	operatorOptionsSetRegister,

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
