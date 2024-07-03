## gopendal

The magic behind is [purego](https://github.com/ebitengine/purego) + [ffi](https://github.com/JupiterRider/ffi).

**required**: Installation of [libffi](https://github.com/libffi/libffi).

```bash
git submodule update --init --recursive

cd opendal/bindings/c
cargo build --release
cp ./target/release/libopendal_c.so ../../../
cd -
CGO_ENABLE=0 go test -v .
```
