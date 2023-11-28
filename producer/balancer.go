package main

import (
	"github.com/segmentio/kafka-go"
)

type SimpleBalancer struct {
	N, P int
}

func (ob *SimpleBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if len(partitions) == 1 {
		return 0
	}
	if ob.N < 100 {
		ob.N++
		return partitions[ob.P]
	}

	if ob.P+1 < len(partitions) {
		ob.P++
		ob.N = 0

	} else {
		ob.P = 0
		ob.N = 0
	}
	ob.N++
	return partitions[ob.P]
}
