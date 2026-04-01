using BenchmarkDotNet.Running;
using DtPipe.Benchmarks;

// Run a specific class:   dotnet run -c Release -- --filter "*ArrowAdo*"
// Run a specific method:  dotnet run -c Release -- --filter "*ConvertValue*"
// Run all:                dotnet run -c Release -- --filter "*"
BenchmarkSwitcher
    .FromAssembly(typeof(FormatDataTransformerBenchmarks).Assembly)
    .Run(args);
