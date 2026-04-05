using BenchmarkDotNet.Attributes;
using DtPipe.Core.Helpers;

namespace DtPipe.Benchmarks;

/// <summary>
/// Compares the pre-compiled ColumnConverterFactory delegate against the legacy
/// ValueConverter.ConvertValue reflection path.
///
/// Both benchmarks loop RowCount times to amortise BDN method call overhead and produce
/// timings comparable to a real writer initialization + write loop.
///
/// Key metric: Mean time/op. The delegate should be significantly faster because
/// the type-specific lambda is compiled once (Expression.Compile) whereas ConvertValue
/// performs Nullable.GetUnderlyingType() + Type.IsInstanceOfType() on every call.
/// </summary>
[MemoryDiagnoser]
public class TypeConverterBenchmarks
{
    private const int RowCount = 3000;

    private Func<object?, object?> _delegate = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Compile once — mirrors what a writer does during InitializeAsync.
        _delegate = ColumnConverterFactory.Build(typeof(string), typeof(int));
    }

    /// <summary>
    /// Baseline: legacy ValueConverter.ConvertValue() — reflection-based dispatch
    /// on every call (Nullable.GetUnderlyingType, IsInstanceOfType, if/else chain).
    /// </summary>
    [Benchmark(Baseline = true)]
    public object? ConvertValueDirectCall()
    {
        object? last = null;
        for (int i = 0; i < RowCount; i++)
            last = ValueConverter.ConvertValue("42", typeof(int));
        return last;
    }

    /// <summary>
    /// Optimized: pre-compiled ColumnConverterFactory delegate.
    /// The type-specific parse lambda was compiled once at writer initialization
    /// and is invoked here with no reflection overhead.
    /// </summary>
    [Benchmark]
    public object? ColumnConverterDelegate()
    {
        object? last = null;
        for (int i = 0; i < RowCount; i++)
            last = _delegate("42");
        return last;
    }
}
