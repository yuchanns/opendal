package opendal_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"opendal"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yuchanns/opendal-go-services/aliyun_drive"
)

type behaviorTest = func(assert *require.Assertions, op *opendal.Operator)

func TestBehavior(t *testing.T) {
	op, err := newOperator()
	require.Nil(t, err)

	cap := op.Info().GetFullCapability()

	var tests []behaviorTest

	tests = append(tests, testsCopy(cap)...)

	for i := range tests {
		test := tests[i]

		fullName := runtime.FuncForPC(reflect.ValueOf(test).Pointer()).Name()
		parts := strings.Split(fullName, ".")
		testName := strings.TrimPrefix(parts[len((parts))-1], "test")

		t.Run(testName, func(t *testing.T) {
			// Run all tests in parallel by default.
			// To run synchronously for specific services, set GOMAXPROCS=1.
			t.Parallel()
			assert := require.New(t)

			test(assert, op)
		})
	}
}

func newOperator() (op *opendal.Operator, err error) {
	var schemes = []opendal.Schemer{
		aliyun_drive.Scheme,
	}

	test := os.Getenv("OPENDAL_TEST")
	var scheme opendal.Schemer
	for _, s := range schemes {
		if s.Scheme() != test {
			continue
		}
		scheme = s
		break
	}
	if scheme == nil {
		err = fmt.Errorf("unsupported scheme: %s", test)
		return
	}

	prefix := fmt.Sprintf("OPENDAL_%s_", strings.ToUpper(scheme.Scheme()))

	opts := opendal.OperatorOptions{}
	for _, env := range os.Environ() {
		pair := strings.SplitN(env, "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := pair[0]
		value := pair[1]
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		opts[strings.ToLower(strings.TrimPrefix(key, prefix))] = value
	}

	op, err = opendal.NewOperator(scheme, opts)
	if err != nil {
		err = fmt.Errorf("create operator must succeed: %s", err)
	}

	return
}

func assertErrorCode(err error) opendal.ErrorCode {
	return err.(*opendal.Error).Code()
}

func genBytesWithRange(min, max uint) ([]byte, uint) {
	diff := max - min
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(diff+1)))
	size := uint(n.Int64()) + min

	content := make([]byte, size)

	_, _ = rand.Read(content)

	return content, size
}

func genBytes(cap *opendal.Capability) ([]byte, uint) {
	maxSize := cap.WriteTotalMaxSize()
	if maxSize == 0 {
		maxSize = 4 * 1024 * 1024
	}
	return genBytesWithRange(1, maxSize)
}
