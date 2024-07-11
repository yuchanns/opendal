package opendal_test

import (
	"opendal"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsRead(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() {
		return nil
	}
	return []behaviorTest{
		testReadFull,
		testReader,
		testReadNotExist,
		testReadWithDirPath,
		testReadWithSpecialChars,
	}
}

func testReadFull(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	bs, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(size, uint(len(bs)), "read size")
	assert.Equal(content, bs, "read content")
}

func testReader(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	r, err := op.Reader(path)
	assert.Nil(err)
	bs, err := r.Read(size)
	assert.Nil(err)
	assert.Equal(size, uint(len(bs)), "read size")
	assert.Equal(content, bs, "read content")
}

func testReadNotExist(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := fixture.NewFilePath()

	_, err := op.Read(path)
	assert.NotNil(err)
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
}

func testReadWithDirPath(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path), "create must succeed")

	_, err := op.Read(path)
	assert.NotNil(err)
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testReadWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, size := fixture.NewFileWithPath(uuid.NewString() + " !@#$%^&()_+-=;',.txt")

	assert.Nil(op.Write(path, content), "write must succeed")

	bs, err := op.Read(path)
	assert.Nil(err)
	assert.Equal(size, uint(len(bs)))
	assert.Equal(content, bs)
}
