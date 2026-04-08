using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using DtPipe.Benchmarks;

// Run a specific class:   dotnet run -c Release -- --filter "*ArrowAdo*"
// Run a specific method:  dotnet run -c Release -- --filter "*BuildConfig*"
// Run all:                dotnet run -c Release -- --filter "*"
BenchmarkSwitcher
    .FromAssembly(typeof(ArrowAdoBenchmarks).Assembly)
    .Run(args, new InProcessConfig());

/// <summary>
/// BenchmarkDotNet configuration applied globally to all benchmark classes in this project.
/// Uses InProcessEmitToolchain to prevent BDN's out-of-process directory scanner from hitting
/// the permission-restricted tests/scripts/artifacts/restricted fixture (d---------).
/// Results are equivalent to the default out-of-process toolchain for micro-benchmarks
/// that do not measure startup time.
/// </summary>
internal sealed class InProcessConfig : ManualConfig
{
    public InProcessConfig()
    {
        AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance));
        AddLogger(ConsoleLogger.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
    }
}
