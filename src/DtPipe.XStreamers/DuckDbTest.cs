using System;
using System.Reflection;
using DuckDB.NET.Data;

public class AdbcTest {
    public static void Main() {
        var methods = typeof(DuckDBConnection).GetMethods(BindingFlags.Public | BindingFlags.Instance);
        foreach(var m in methods) {
            Console.WriteLine(m.Name);
        }
    }
}
