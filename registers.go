package opendal

type operatorRegister func(uintptr, *Operator) error

var operatorRegisters = []operatorRegister{
	operatorReadRegister,

	operatorWriteRegister,
}
