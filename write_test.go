package opendal_test

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.yuchanns.xyz/opendal"
)

func testsWrite(cap *opendal.Capability) []behaviorTest {
	if !cap.Write() || !cap.Stat() || !cap.Read() {
		return nil
	}
	return []behaviorTest{
		testWriteOnly,
		testWriteWithEmptyContent,
		testWriteWithDirPath,
		testWriteWithSpecialChars,
		testWriteOverwrite,
	}
}

func testWriteOnly(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content))

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(size), meta.ContentLength())
}

func testWriteWithEmptyContent(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().WriteCanEmpty() {
		return
	}

	path := fixture.NewFilePath()
	assert.Nil(op.Write(path, []byte{}))

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(0), meta.ContentLength())
}

func testWriteWithDirPath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewDirPath()

	err := op.Write(path, []byte("1"))
	assert.NotNil(err)
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testWriteWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFileWithPath(uuid.NewString() + " !@#$%^&()_+-=;',.txt")

	assert.Nil(op.Write(path, content))

	meta, err := op.Stat(path)
	assert.Nil(err, "stat must succeed")
	assert.Equal(uint64(size), meta.ContentLength())
}

func testWriteOverwrite(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().WriteCanMulti() {
		return
	}

	path := fixture.NewFilePath()
	size := uint(5 * 1024 * 1024)
	contentOne, contentTwo := genFixedBytes(size), genFixedBytes(size)

	assert.Nil(op.Write(path, contentOne))
	bs, err := op.Read(path)
	assert.Nil(err, "read must succeed")
	assert.Equal(contentOne, bs, "read content_one")

	assert.Nil(op.Write(path, contentTwo))
	bs, err = op.Read(path)
	assert.Nil(err, "read must succeed")
	assert.NotEqual(contentOne, bs, "content_one must be overwrote")
	assert.Equal(contentTwo, bs, "read content_two")
}
