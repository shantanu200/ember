package quelon

import "testing"

func BenchmarkPartitionFor(b *testing.B) {
	p := &Pool{partitions: 16}
	b.ReportAllocs()
	for b.Loop() {
		_ = p.partitionFor("some-key-value")
	}
}
