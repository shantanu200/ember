package quelon

import "testing"

func BenchmarkPartitionFor(b *testing.B) {
	p := &Pool{partitions: 16}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.partitionFor("some-key-value")
	}
}
