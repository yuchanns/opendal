package opendal_test

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.yuchanns.xyz/opendal"
)

func testsCopy(cap *opendal.Capability) []behaviorTest {
	if !cap.Read() || !cap.Write() || !cap.Copy() {
		return nil
	}
	return []behaviorTest{
		testCopyFileWithASCIIName,
		testCopyFileWithNonASCIIName,
		testCopyNonExistingSource,
		testCopySourceDir,
		testCopyTargetDir,
		testCopySelf,
		testCopyNested,
		testCopyOverwrite,
	}
}

func testCopyFileWithASCIIName(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := fixture.NewFilePath()

	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)
}

func testCopyFileWithNonASCIIName(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFileWithPath("🐂🍺中文.docx")
	targetPath := fixture.PushPath("😈🐅Français.docx")

	assert.Nil(op.Write(sourcePath, sourceContent))
	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)
}

func testCopyNonExistingSource(assert *require.Assertions, op *opendal.Operator, _ *fixture) {
	sourcePath := uuid.NewString()
	targetPath := uuid.NewString()

	err := op.Copy(sourcePath, targetPath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeNotFound, assertErrorCode(err))
}

func testCopySourceDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	sourcePath := fixture.NewDirPath()
	targetPath := uuid.NewString()

	assert.Nil(op.CreateDir(sourcePath))

	err := op.Copy(sourcePath, targetPath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testCopyTargetDir(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	if !op.Info().GetFullCapability().CreateDir() {
		return
	}

	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := fixture.NewDirPath()

	assert.Nil(op.CreateDir(targetPath))

	err := op.Copy(sourcePath, targetPath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeIsADirectory, assertErrorCode(err))
}

func testCopySelf(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent))

	err := op.Copy(sourcePath, sourcePath)
	assert.NotNil(err, "copy must fail")
	assert.Equal(opendal.CodeIsSameFile, assertErrorCode(err))
}

func testCopyNested(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath := fixture.PushPath(fmt.Sprintf(
		"%s/%s/%s",
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
	))

	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)
}

func testCopyOverwrite(assert *require.Assertions, op *opendal.Operator, fixture *fixture) {
	sourcePath, sourceContent, _ := fixture.NewFile()

	assert.Nil(op.Write(sourcePath, sourceContent))

	targetPath, targetContent, _ := fixture.NewFile()
	assert.NotEqual(sourceContent, targetContent)

	assert.Nil(op.Write(targetPath, targetContent))

	assert.Nil(op.Copy(sourcePath, targetPath))

	targetContent, err := op.Read(targetPath)
	assert.Nil(err, "read must succeed")
	assert.Equal(sourceContent, targetContent)
}
