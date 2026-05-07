#r "src/DtPipe.Adapters/bin/Release/net10.0/Parquet.dll"
using System;
using System.Reflection;
using System.Linq;

var assembly = Assembly.LoadFrom("src/DtPipe.Adapters/bin/Release/net10.0/Parquet.dll");
foreach (var type in assembly.GetTypes().Where(t => t.Name.Contains("Column")))
{
    Console.WriteLine(type.FullName);
}
