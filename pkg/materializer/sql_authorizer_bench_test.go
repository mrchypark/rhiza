package materializer

import (
	"fmt"
	"testing"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
)

func BenchmarkSQLAuthorizerSchemaCount(b *testing.B) {
	for _, count := range []int{1, 64, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			m, err := Open(b.TempDir()+"/schema.db", 1)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Close()
			for i := range count {
				if _, err := m.db.Exec(fmt.Sprintf("CREATE TABLE table_%d (id INTEGER)", i)); err != nil {
					b.Fatal(err)
				}
			}
			conn, err := m.db.Conn(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()
			var rawConn any
			if err := conn.Raw(func(raw any) error {
				rawConn = raw.(driver.Conn).Raw()
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				set, err := sqlAuthorizer(rawConn.(*sqlite3.Conn), true, false)
				if err != nil {
					b.Fatal(err)
				}
				if err := set(false, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
