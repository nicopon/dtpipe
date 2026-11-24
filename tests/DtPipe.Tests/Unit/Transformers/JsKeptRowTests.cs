using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Services;
using Jint.Runtime;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// A script keeps nothing from one call to the next. Its engine's realm is frozen when the engine is
/// made (<see cref="JsRealmFreeze"/>), so a script that stores its row in a global fails loudly, naming
/// the write refused, on each transformer and each path, rather than reading on a later call a row the
/// transformer has moved on from. A window's array is the script's for one call. Errors read the same
/// on both paths.
/// </summary>
public class JsKeptRowTests : IDisposable
{
    private const string WriteRefused = "*Cannot assign to read only property '__p' of Object*";

    private static readonly IReadOnlyList<PipeColumnInfo> Columns = [new("A", typeof(double), false)];

    private readonly JsEngineProvider _js = new();

    public void Dispose() => _js.Dispose();

    // ── A row kept in a global ───────────────────────────────────────────────

    public static TheoryData<string, string> KeepingScripts => new()
    {
        { "compute", "const p = globalThis.__p; globalThis.__p = row; return p ? row.A - p.A : null;" },
        { "filter", "const p = globalThis.__p; globalThis.__p = row; return p == null || row.A - p.A > 1;" },
        { "expand", "const p = globalThis.__p; globalThis.__p = row; return [{ ...row, D: p ? row.A - p.A : null }];" },
        { "window", "const p = globalThis.__p; globalThis.__p = rows[0]; return [{ A: p ? rows[0].A - p.A : -1 }];" },
        { "window-key", "const p = globalThis.__p; globalThis.__p = rows[0]; return rows.map(r => ({ A: p ? p.A : -1 }));" },
        { "window", "globalThis.__p = (globalThis.__p || []).concat(rows); return rows.map(r => ({ A: globalThis.__p[0].A }));" },
    };

    [Theory]
    [MemberData(nameof(KeepingScripts))]
    public async Task A_Script_Keeping_Its_Row_In_A_Global_Fails_Naming_The_Write_On_Both_Paths(string kind, string script)
    {
        var onRows = async () => RunRows(await CreateAsync(kind, script, _js), [1, 2, 4]);
        (await onRows.Should().ThrowAsync<InvalidOperationException>()).WithMessage(WriteRefused);

        var onBatches = async () => await RunAsync(await CreateAsync(kind, script, _js), [[1, 2], [4]], column: 0);
        (await onBatches.Should().ThrowAsync<InvalidOperationException>()).WithMessage(WriteRefused);
    }

    // ── A window's array ─────────────────────────────────────────────────────

    /// <summary>A window's array is the script's for one call: what it adds does not reach the next window.</summary>
    [Fact]
    public async Task Window_A_Change_To_The_Array_Stays_In_Its_Window_On_Both_Paths()
    {
        const string script = "rows.push({ A: rows.reduce((s, r) => s + r.A, 0) }); return rows;";

        var rows = await CreateAsync("window", script, _js, count: 2);
        RunRows(rows, [1, 2, 4, 8]).Should().Equal(1d, 2d, 3d, 4d, 8d, 12d);

        var batches = await CreateAsync("window", script, _js, count: 2);
        (await RunAsync(batches, [[1, 2, 4, 8]], column: 0)).Should().Equal(1d, 2d, 3d, 4d, 8d, 12d);
    }

    // ── Errors read the same on both paths ───────────────────────────────────

    public static TheoryData<string, string, string> Failures => new()
    {
        { "compute", "return row.Missing;", "*Column 'Missing' not found in schema*" },
        { "filter", "return row.Missing > 0;", "*Column 'Missing' not found in schema*A filter reads its columns off 'row'*" },
        { "expand", "return [{ A: row.Missing }];", "*--expand could not evaluate*Column 'Missing' not found in schema*" },
        { "window", "return [{ A: rows[0].Missing }];", "*Error evaluating window script*Column 'Missing' not found in schema*" },
    };

    [Theory]
    [MemberData(nameof(Failures))]
    public async Task An_Unknown_Column_Fails_Alike_On_Either_Path(string kind, string body, string message)
    {
        var transformer = await CreateAsync(kind, body, _js);

        var onRows = () => RunRows(transformer, [1]);
        onRows.Should().Throw<InvalidOperationException>().WithMessage(message);

        var onBatches = async () => await RunAsync(transformer, [[1]], column: 0);
        (await onBatches.Should().ThrowAsync<InvalidOperationException>()).WithMessage(message);
    }

    [Theory]
    [InlineData("compute")]
    [InlineData("filter")]
    [InlineData("expand")]
    [InlineData("window")]
    public async Task A_Timeout_And_A_Memory_Limit_Fail_The_Run(string kind)
    {
        using var limited = new LimitedJsEngineProvider();

        var looping = await CreateAsync(kind, "while (true) {} return [];", limited);
        var loop = async () => await RunAsync(looping, [[1]], column: 0);
        (await loop.Should().ThrowAsync<InvalidOperationException>()).WithInnerException<TimeoutException>();

        var growing = await CreateAsync(kind, "const a = []; while (true) a.push(new Array(100000).fill(1)); return [];", limited);
        var grow = async () => await RunAsync(growing, [[1]], column: 0);
        (await grow.Should().ThrowAsync<InvalidOperationException>()).WithInnerException<MemoryLimitExceededException>();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static async Task<BaseColumnarTransformer> CreateAsync(string kind, string script, IJsEngineProvider js, int count = 1)
    {
        BaseColumnarTransformer transformer = kind switch
        {
            "compute" => new ComputeDataTransformer(new ComputeOptions { Compute = [$"D:double:{script}"] }, js),
            "filter" => new FilterDataTransformer(new FilterOptions { Filters = [script] }, js),
            "expand" => new ExpandDataTransformer(new ExpandOptions { Expand = [script] }, js),
            "window" => new WindowDataTransformer(new WindowOptions { Count = count, Script = script }, js),
            "window-key" => new WindowDataTransformer(new WindowOptions { Key = "A", Script = script }, js),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
        await transformer.InitializeAsync(Columns);
        return transformer;
    }

    /// <summary>Runs <paramref name="values"/> through the row path, its flush included, and returns column 0 of every row it emits.</summary>
    private static List<object?> RunRows(BaseColumnarTransformer transformer, double[] values)
    {
        var output = new List<object?>();
        foreach (var a in values)
        {
            if (transformer is IMultiRowTransformer multi)
                output.AddRange(multi.TransformMany([a]).Select(r => r[0]));
            else if (transformer.Transform([a]) is { } row)
                output.Add(row[0]);
        }
        output.AddRange(transformer.Flush().Select(r => r[0]));
        return output;
    }

    /// <summary>
    /// Runs <paramref name="transformer"/> over one batch per array, disposing each input once the
    /// transformer has run, as the segment runner does, and returns <paramref name="column"/> of every
    /// row it emits, its flush included.
    /// </summary>
    private static async Task<List<object?>> RunAsync(BaseColumnarTransformer transformer, double[][] batches, int column)
    {
        var output = new List<object?>();
        foreach (var values in batches)
        {
            var input = Batch(values);
            var result = await transformer.TransformBatchAsync(input);
            output.AddRange(Read(result, column));
            if (!ReferenceEquals(result, input))
                result?.Dispose();
            input.Dispose();
        }

        await foreach (var flushed in transformer.FlushBatchAsync())
        {
            using (flushed)
                output.AddRange(Read(flushed, column));
        }
        return output;
    }

    private static IEnumerable<object?> Read(RecordBatch? batch, int column)
    {
        if (batch is null)
            return [];
        var array = batch.Column(column);
        var field = batch.Schema.GetFieldByIndex(column);
        return Enumerable.Range(0, batch.Length).Select(i => ArrowTypeMapper.GetValueForField(array, field, i)).ToList();
    }

    private static RecordBatch Batch(double[] values)
    {
        var builder = new DoubleArray.Builder();
        builder.AppendRange(values);
        return new RecordBatch(new Schema([new Field("A", DoubleType.Default, false)], null), [builder.Build()], values.Length);
    }
}
