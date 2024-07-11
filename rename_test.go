package opendal_test

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.yuchanns.xyz/opendal"
)

func testsRename(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() || !cap.Rename() {
		return nil
	}
	return []behaviorTest{
		testRenameFile,
		testRenameNonExistingSource,
		testRenameSourceDir,
		testRenameTargetDir,
		testRenameSelf,
		testRenameNested,
		testRenameOverwrite,
	}
}

func testRenameFile(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent), "write must succeed")

	targetPath := fixture.NewFilePath()

	assert.Nil(op.Rename(sourcePath, targetPath))

	_, err := op.Stat(sourcePath)
	assert.NotNil(err, "stat must fail")
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err)
	assert.Equal(sourceContent, targetContent)
}

func testRenameNonExistingSource(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath := fixture.NewFilePath()
	targetPath := fixture.NewFilePath()

	err := op.Rename(sourcePath, targetPath)
	assert.NotNil(err, "rename must fail")
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
}

func testRenameSourceDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	sourcePath := fixture.NewDirPath()
	targetPth := fixture.NewFilePath()

	assert.Nil(op.CreateDir(sourcePath), "create must succeed")

	err := op.Rename(sourcePath, targetPth)
	assert.NotNil(err, "rename must fail")
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testRenameTargetDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent), "write must succeed")

	targetPath := fixture.NewDirPath()
	assert.Nil(op.CreateDir(targetPath))

	err := op.Rename(sourcePath, targetPath)
	assert.NotNil(err)
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testRenameSelf(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent), "write must succeed")

	err := op.Rename(sourcePath, sourcePath)
	assert.NotNil(err)
	assert.Equal(opendal.CodeIsSameFile, assertErrorCode(err))
}

func testRenameNested(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent), "write must succeed")

	targetPath := fixture.PushPath(fmt.Sprintf(
		"%s/%s/%s",
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
	))

	assert.Nil(op.Rename(sourcePath, targetPath))

	_, err := op.Stat(sourcePath)
	assert.NotNil(err, "stat must fail")
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)
}

func testRenameOverwrite(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent), "write must succeed")

	targetPath, targetContent, _ := fixture.NewFile()
	assert.NotEqual(sourceContent, targetContent)

	assert.Nil(op.Write(targetPath, targetContent), "write must succeed")

	assert.Nil(op.Rename(sourcePath, targetPath))

	_, err := op.Stat(sourcePath)
	assert.NotNil(err, "stat must fail")
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))

	targetContent, err = op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)
}
