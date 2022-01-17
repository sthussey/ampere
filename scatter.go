package ampere

import (
	"fmt"
	"runtime"
	"sync"
)

// For some dataset, it is partitionable
// if it can be chunked into partitions of type T
// addressed by an int index
type Partitionable[T any] interface {
	Partition(int) (T, error)
}

type PartitionResult[PR any] struct {
	Result           PR
	PartitionAddress int
	Err              error
}

type Scatterer[D Partitionable[P], P any, PR any, R any] struct {
	// The partitioner will, given a dataset D, return a slice of partition addresses
	Partitioner func(D) []int
	// The scatter will call the Runnable with a data partition in a goroutine and collect the result type R
	Runnable func(P) (PR, error)
	// Gatherer will combine an array of partition results. Optional
	Gatherer func([]PartitionResult[PR]) R
}

func (s *Scatterer[D, P, PR, R]) Scatter(data D) (R, error) {
	var wg sync.WaitGroup

	partitions := s.Partitioner(data)
	results := make([]PartitionResult[PR], len(partitions))

	resultChan := make(chan PartitionResult[PR], len(partitions))

	for i := range partitions {
		part, err := data.Partition(i)
		if err != nil {
			results[i] = PartitionResult[PR]{Err: fmt.Errorf("error getting partition %d: %s", i, err)}
			continue
		}
		wg.Add(1)
		go func(data P, idx int) {
			// TODO - support timeouts
			defer wg.Done()
			result := PartitionResult[PR]{PartitionAddress: idx}
			result.Result, result.Err = s.Runnable(data)
			resultChan <- result
		}(part, i)
	}
	wg.Wait()

	for i := 0; i < len(partitions); i++ {
		partResult := <-resultChan
		results[partResult.PartitionAddress] = partResult
	}

	fullResult := s.Gatherer(results)
	return fullResult, nil
}

// One partition address per OS thread available for goroutines
func BalancedPartitions[D any]() func(D) []int {
	return FactorPartitions[D](1)
}

// Increase/Descrease OS threads by a factor. A factor of > 1
// is useful for a Runnable that is I/O or network bound. < 1
// is useful when you have additional goroutines running aside
// from the scatter set
func FactorPartitions[D any](factor float32) func(D) []int {
	return func(data D) []int {
		goroutineCount := int(factor * float32(runtime.NumCPU()))
		partitions := make([]int, goroutineCount)
		for i := 0; i < goroutineCount; i++ {
			partitions[i] = i
		}
		return partitions
	}
}
