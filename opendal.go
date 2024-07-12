package opendal

import (
	"context"
)

// Scheme defines the interface for storage scheme implementations.
//
// A Scheme represents a specific storage backend (e.g., S3, filesystem, memory)
// and provides methods to identify and initialize the scheme.
//
// Implementations of this interface should be thread-safe, especially the LoadOnce method.
type Scheme interface {
	// Name returns the unique identifier of the scheme.
	Name() string

	// Path returns the filesystem path where the scheme's shared library (.so) is located.
	Path() string

	// LoadOnce initializes the scheme. It ensures that initialization occurs only once,
	// even if called multiple times. Subsequent calls after the first should be no-ops.
	//
	// Returns an error if initialization fails.
	LoadOnce() error
}

// OperatorOptions contains configuration parameters for creating an Operator.
//
// This struct allows users to specify various settings and credentials
// required for connecting to and interacting with different storage backends.
//
// Fields in this struct vary depending on the storage scheme being used.
// Refer to the documentation of specific storage backends for details on
// required and optional fields.
//
// Example usage:
//
//	options := opendal.OperatorOptions{
//		"root": "/path/to/root",
//		"endpoint": "https://example.com",
//		"access_key_id": "your_access_key",
//		"secret_access_key": "your_secret_key",
//	}
type OperatorOptions map[string]string

// Operator is the entry point for all public APIs in OpenDAL.
//
// Operator provides a unified interface for interacting with various storage services.
// It encapsulates the underlying storage operations and presents a consistent API
// regardless of the storage backend.
//
// # Usage
//
// Create an Operator using NewOperator, perform operations, and always remember
// to Close the operator when finished to release resources.
//
// # Example
//
//	func main() {
//		// Create a new operator for the memory backend
//		op, err := opendal.NewOperator(memory.Scheme, opendal.OperatorOptions{})
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer op.Close() // Ensure the operator is closed when done
//
//		// Perform operations using the operator
//		err = op.Write("example.txt", []byte("Hello, OpenDAL!"))
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		data, err := op.Read("example.txt")
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Println(string(data))
//	}
//
// Note: Always use defer op.Close() to ensure proper resource cleanup.
//
// # Available Operations
//
// Operator provides methods for common storage operations including:
//   - Read: Read data from a path
//   - Write: Write data to a path
//   - Stat: Get metadata for a path
//   - Delete: Remove a file or directory
//   - List: Enumerate entries in a directory
//   - and more...
//
// Refer to the individual method documentation for detailed usage information.
type Operator struct {
	ctx    context.Context
	cancel context.CancelFunc

	inner *opendalOperator
}

// NewOperator creates and initializes a new Operator for the specified storage scheme.
//
// Parameters:
//   - scheme: The storage scheme (e.g., "memory", "s3", "fs").
//   - options: Configuration options for the operator.
//
// Returns:
//   - *Operator: A new Operator instance.
//   - error: An error if initialization fails, or nil if successful.
//
// Note: Remember to call Close() on the returned Operator when it's no longer needed.
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

// Close releases all resources associated with the Operator.
//
// It's important to call this method when the Operator is no longer needed
// to ensure proper cleanup of underlying resources.
//
// Note: It's recommended to use defer op.Close() immediately after creating an Operator.
func (op *Operator) Close() {
	free := getFFI[operatorFree]
	free(op.ctx, symOperatorFree)(op.inner)
	op.cancel()
}
