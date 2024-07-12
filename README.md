## opendal go binding

![behavior_test](https://github.com/yuchanns/opendal/workflows/behavior_test/badge.svg?branch=main)

```bash
go get go.yuchanns.xyz/opendal
```

The magic behind is [purego](https://github.com/ebitengine/purego) + [ffi](https://github.com/JupiterRider/ffi).

**required**: Installation of [libffi](https://github.com/libffi/libffi).

## Basic Usage

```go
package main

import (
	"fmt"
	"os"

	"github.com/yuchanns/opendal-go-services/memory"
	"go.yuchanns.xyz/opendal"
)

func main() {
	op, _ := opendal.NewOperator(memory.Scheme, opendal.OperatorOptions{})
	// Write to /opendal/test
	op.Write("test", []byte("Hello opendal go binding!"))
	// Read from /opendal/test
	data, _ := op.Read("test")
	fmt.Printf("read: %s", data)
	// List under /opendal
	lister, _ := op.List("/")
	// Iteratable Lister
	for lister.Next() {
		entry := lister.Entry()
		if err := entry.Error(); err != nil {
			panic(err)
		}
		_ = entry.Name()
		// Stat entry
		meta, _ := op.Stat(entry.Path())
		// length
		fmt.Printf("len: %d\n", meta.ContentLength())
		// modified time
		fmt.Printf("updated: %s\n", meta.LastModified())
		// check file type
		fmt.Printf("dir: %v, file %v", meta.IsDir(), meta.IsFile())
	}
	// Copy
	op.Copy("test", "test_copy")
	// Rename
	op.Rename("test", "test_rename")
	// Delete
	op.Delete("test_rename")
}

```

## Run Tests

```bash
# Run all tests
CGO_ENABLE=0 go test -v -run TestBehavior
# Run specific test
CGO_ENABLE=0 go test -v -run TestBehavior/Write
# Run synchronously
CGO_ENABLE=0 GOMAXPROCS=1 go test -v -run TestBehavior
```

## Capabilities

- [x] OperatorInfo
- [x] Stat
    - [x] Metadata
- [x] IsExist
- [x] Read
    - [x] Read
    - [x] Reader
- [ ] Write
    - [x] Write
    - [ ] Reader -- Need support from the C binding
- [x] Delete
- [x] CreateDir
- [ ] Lister
    - [x] Entry
    - [ ] Metadata -- Need support from the C binding
- [x] Copy
- [x] Rename

