# filestore

```
$ go test -bench=. -benchmem ./pkg/store/filestore/
goos: linux
goarch: arm64
pkg: github.com/mrchypark/daramjwee/pkg/store/filestore
BenchmarkFileStore_Set_RenameStrategy-8            26770             39491 ns/op            1418 B/op         25 allocs/op
BenchmarkFileStore_Set_CopyStrategy-8              24439             48586 ns/op            1450 B/op         32 allocs/op
BenchmarkFileStore_Get_RenameStrategy-8           137865              8158 ns/op            1402 B/op         20 allocs/op
BenchmarkFileStore_Get_CopyStrategy-8             152445              7706 ns/op            1402 B/op         20 allocs/op
PASS
ok      github.com/mrchypark/daramjwee/pkg/store/filestore      10.002s

$ go test -bench=. -benchmem ./pkg/store/filestore/
goos: linux
goarch: arm64
pkg: github.com/mrchypark/daramjwee/pkg/store/filestore
BenchmarkFileStore_Set_RenameStrategy-8            38035             27859 ns/op            1193 B/op         22 allocs/op
BenchmarkFileStore_Set_CopyStrategy-8              32852             42279 ns/op            1049 B/op         26 allocs/op
BenchmarkFileStore_Get_RenameStrategy-8           267121              4379 ns/op             549 B/op         16 allocs/op
BenchmarkFileStore_Get_CopyStrategy-8             269330              4312 ns/op             549 B/op         16 allocs/op
PASS
ok      github.com/mrchypark/daramjwee/pkg/store/filestore      6.558s
```