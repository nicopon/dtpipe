using System;
using System.Reflection;
using DuckDB.NET.Data;

class Program
{
    static void Main()
    {
        // Try getting the implementation type if DuckDBAppenderRow is not where methods are
        var type = typeof(DuckDBAppender).Assembly.GetType("DuckDB.NET.Data.DuckDBAppenderRow");
        if (type == null) type = typeof(DuckDBConnection).Assembly.GetType("DuckDB.NET.Data.DuckDBAppenderRow");

        if (type != null)
        {
            var methods = type.GetMethods();
            foreach (var m in methods)
            {
                if (m.Name.Contains("Append"))
                {
                    var ps = m.GetParameters();
                    string pStr = "";
                    foreach(var p in ps) pStr += p.ParameterType.Name + " " + p.Name + ", ";
                    Console.WriteLine($"{m.Name}({pStr})");
                }
            }
        }
    }
}
