package opendal_test

import (
	"opendal"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testsDelete(cap *opendal.Capability) []behaviorTest {
	if !cap.Stat() || !cap.Delete() || !cap.Write() {
		return nil
	}
	tests := []behaviorTest{
		testDeleteFile,
		testDeleteEmptyDir,
		testDeleteWithSpecialChars,
		testDeleteNotExisting,
	}
	return tests
}

func testDeleteFile(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path, content, _ := fixture.NewFile()

	assert.Nil(op.Write(path, content), "write must succeed")

	assert.Nil(op.Delete(path))

	assert.False(op.IsExist(path))
}

func testDeleteEmptyDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	path := fixture.NewDirPath()

	assert.Nil(op.CreateDir(path), "create must succeed")

	assert.Nil(op.Delete(path))
}

func testDeleteWithSpecialChars(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := uuid.NewString() + " !@#$%^&()_+-=;',.txt"
	path, content, _ := fixture.NewFileWithPath(path)

	assert.Nil(op.Write(path, content), "write must succeed")

	assert.Nil(op.Delete(path))

	assert.False(op.IsExist(path))
}

func testDeleteNotExisting(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	path := uuid.NewString()

	assert.Nil(op.Delete(path))
}
