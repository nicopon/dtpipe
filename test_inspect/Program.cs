using System;
using System.Collections.Generic;
using Apache.Arrow.Adbc;

public class AdbcTest {
    public static void Main() {
        try {
            Console.WriteLine("Loading DuckDB ADBC Driver...");
            // DuckDB extension entry point is duckdb_adbc_init
            var driver = AdbcDriver.Load("duckdb", "duckdb_adbc_init");
            using var db = driver.Open(new Dictionary<string, string>{ { "path", ":memory:" } });
            using var conn = db.Connect(new Dictionary<string, string>());
            using var stmt = conn.CreateStatement();
            stmt.SqlQuery = "SELECT 42 as Answer";
            var result = stmt.ExecuteQuery();
            Console.WriteLine("Query executed successfully: ADBC is working natively!");
        } catch (Exception e) {
            Console.WriteLine("Error: " + e.Message);
        }
    }
}
