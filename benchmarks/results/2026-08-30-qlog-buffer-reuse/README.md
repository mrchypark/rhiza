# QLog scan scratch buffer reuse

Environment: Apple M3, darwin/arm64, Go 1.27.0. The benchmark scans 256 WAL
entries with 4 KiB payloads. Command:

```bash
go test ./pkg/qlog -run '^$' -bench '^BenchmarkWALScanScratch$' -benchmem
```

| implementation | bytes/op | allocs/op | observed ns/op |
| --- | ---: | ---: | ---: |
| heap scratch per entry | ~2,294,000 | 513 | 22,713,649–65,188,479 |
| arena scratch per scan | 2,700,385 | 259 | 28,470,167 |
| reusable heap scratch buffer | 1,053,712 | 259 | 26,331,666–68,332,413 |

Wall-clock results were noisy on the shared host, so the change is justified
only by the stable allocation result: one reusable `[]byte` matches the arena
allocation count while using about 61% fewer bytes than the arena build. It
also removes the experimental build split and keeps decoded payload ownership
unchanged.
