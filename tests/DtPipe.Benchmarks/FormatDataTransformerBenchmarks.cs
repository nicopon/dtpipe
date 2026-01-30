using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using DtPipe.Transformers.Format;
using DtPipe.Core;
using DtPipe.Core.Options;
using DtPipe.Core.Models;
namespace DtPipe.Benchmarks;

[MemoryDiagnoser]
public class FormatDataTransformerBenchmarks
{
    private FormatDataTransformer _transformer = null!;
    private object?[] _row = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var options = new FormatOptions { Format = new[] { "FULLNAME:{FIRST} {LAST}", "PRICE_FMT:{PRICE:0.00} USD" } };
        _transformer = new FormatDataTransformer(options);
        var columns = new List<ColumnInfo>
        {
            new("FIRST", typeof(string), true),
            new("LAST", typeof(string), true),
            new("PRICE", typeof(decimal), true),
            new("FULLNAME", typeof(string), true),
            new("PRICE_FMT", typeof(string), true)
        };
        
        await _transformer.InitializeAsync(columns);
        _row = new object?[] { "John", "Doe", 123.456m, null, null };
    }

    [Benchmark]
    public object?[] Transform()
    {
        return _transformer.Transform(_row);
    }
}
