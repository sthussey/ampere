package ampere

import (
	"runtime"
	"testing"
	"github.com/stretchr/testify/assert"
)

type EchoIndex struct {}

func (ei EchoIndex) Partition(idx int) (int, error) {
	return idx, nil
}

func TestBalancedScatterer(t *testing.T) {
	var s Scatterer[EchoIndex, int, int, []int]
	s.Partitioner = BalancedPartitions[EchoIndex]()
	s.Runnable = func(i int) (int, error) {
		return 2 * i, nil
	}
	s.Gatherer = func(pr []PartitionResult[int]) []int {
		results := make([]int, len(pr))
		for i := range pr {
			results[pr[i].PartitionAddress] = pr[i].Result
		}
		return results
	}

	d := EchoIndex{}
	r, err := s.Scatter(d)

	assert.Nil(t, err, "Err should be nil")

	assert.Equal(t, len(r), runtime.NumCPU(), "Result should the same elements as available CPUs")

	for i := range(r) {
		assert.Equalf(t, r[i], i * 2, "Result element %d should be equal to %d", i, i * 2)
	}
}
