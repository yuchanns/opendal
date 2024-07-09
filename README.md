## gopendal

The magic behind is [purego](https://github.com/ebitengine/purego) + [ffi](https://github.com/JupiterRider/ffi).

**required**: Installation of [libffi](https://github.com/libffi/libffi).

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
- [x] Reader
    - [x] Read
    - [x] ReaderRead
- [x] Write
- [x] Delete
- [x] CreateDir
- [ ] Lister
    - [x] Entry
    - [ ] Metadata -- Need support from the C binding
- [x] Copy
- [x] Rename

