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
using Jint;
using Jint.Runtime;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// A script that keeps its row sees that row on a later call, not the one the transformer has moved
/// to: each row is a value, on the row path and on the Arrow path, across batches the pipeline has
/// already disposed. A script that cannot keep it stays on the view the transformer reuses.
/// </summary>
public class JsKeptRowTests : IDisposable
{
    private const string KeepsPrevious = "const p = globalThis.__p; globalThis.__p = row; return p ? row.A - p.A : null;";

    private static readonly IReadOnlyList<PipeColumnInfo> Columns = [new("A", typeof(double), false)];

    private readonly JsEngineProvider _js = new();

    public void Dispose() => _js.Dispose();

    // ── Compute ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Compute_A_Script_Keeping_Its_Row_Sees_Each_Row_On_The_Row_Path()
    {
        var compute = await ComputeAsync($"D:double:{KeepsPrevious}");

        compute.SharesRowView.Should().BeFalse();
        new[] { 1d, 2, 4 }.Select(a => compute.Transform([a])![1]).Should().Equal(null, 1d, 2d);
    }

    [Fact]
    public async Task Compute_A_Script_Keeping_Its_Row_Sees_Each_Row_On_The_Arrow_Path()
    {
        var compute = await ComputeAsync($"D:double:{KeepsPrevious}");

        (await RunAsync(compute, [[1, 2], [4]], column: 1)).Should().Equal(null, 1d, 2d);
    }

    /// <summary>A write belongs to the row it is made on: two kept rows do not share it.</summary>
    [Fact]
    public async Task Compute_A_Kept_Row_Keeps_Its_Own_Writes_On_Both_Paths()
    {
        const string script = "D:row.Seen = row.A; globalThis.__kept = (globalThis.__kept || []).concat([row]); "
            + "return globalThis.__kept.map(r => r.Seen).join(',');";

        var rows = await ComputeAsync(script);
        new[] { 1d, 2, 4 }.Select(a => rows.Transform([a])![1]).Should().Equal("1", "1,2", "1,2,4");

        _js.GetEngine().Evaluate("globalThis.__kept = undefined;");
        var batches = await ComputeAsync(script);
        (await RunAsync(batches, [[1, 2], [4]], column: 1)).Should().Equal("1", "1,2", "1,2,4");
    }

    /// <summary>
    /// A kept row read its values when it was made: the batch it came from is freed once the chain has
    /// run, and the row still reads what it held.
    /// </summary>
    [Fact]
    public async Task Compute_A_Kept_Row_Outlives_Its_Batch_Without_Holding_It()
    {
        var pool = new TrackingMemoryPool();
        var compute = await ComputeAsync("D:double:globalThis.__kept = (globalThis.__kept || []).concat([row]); return 0;");

        foreach (var values in new[] { new double[] { 1, 2 }, [4] })
        {
            using var input = Batch(values, pool);
            using var output = await compute.TransformBatchAsync(input);
        }

        pool.ActiveAllocations.Should().Be(0);
        _js.GetEngine().Evaluate("globalThis.__kept.map(r => r.A).join(',')").AsString().Should().Be("1,2,4");
    }

    /// <summary>A script that keeps nothing but is not run in parallel still reads every row on its own view.</summary>
    [Fact]
    public async Task Compute_A_View_Per_Row_Reads_Every_Row_On_The_Parallel_Path()
    {
        const int n = 2048;
        var compute = await ComputeAsync("D:double:(function* () {})() && row.A * 2");
        compute.SharesRowView.Should().BeFalse();

        var values = Enumerable.Range(1, n).Select(i => (double)i).ToArray();
        (await RunAsync(compute, [values], column: 1)).Should().Equal(values.Select(v => (object?)(v * 2)));
    }

    // ── Filter ───────────────────────────────────────────────────────────────

    private const string KeepsPreviousFilter = "const p = globalThis.__p; globalThis.__p = row; return p == null || row.A - p.A > 1;";

    [Fact]
    public async Task Filter_A_Script_Keeping_Its_Row_Sees_Each_Row_On_The_Row_Path()
    {
        var filter = await FilterAsync(KeepsPreviousFilter);

        filter.SharesRowView.Should().BeFalse();
        new[] { 1d, 2, 4 }.Where(a => filter.Transform([a]) is not null).Should().Equal(1d, 4d);
    }

    [Fact]
    public async Task Filter_A_Script_Keeping_Its_Row_Sees_Each_Row_On_The_Arrow_Path()
    {
        var filter = await FilterAsync(KeepsPreviousFilter);

        (await RunAsync(filter, [[1, 2], [4]], column: 0)).Should().Equal(1d, 4d);
    }

    // ── Expand ───────────────────────────────────────────────────────────────

    private const string KeepsPreviousExpand =
        "const p = globalThis.__p; globalThis.__p = row; return [{ ...row, D: p ? row.A - p.A : null }];";

    [Fact]
    public async Task Expand_A_Script_Keeping_Its_Row_Sees_Each_Row_On_The_Row_Path()
    {
        var expand = await ExpandAsync(KeepsPreviousExpand);

        expand.SharesRowView.Should().BeFalse();
        new[] { 1d, 2, 4 }.Select(a => expand.TransformMany([a]).Single()[1]).Should().Equal(null, 1d, 2d);
    }

    [Fact]
    public async Task Expand_A_Script_Keeping_Its_Row_Sees_Each_Row_On_The_Arrow_Path()
    {
        var expand = await ExpandAsync(KeepsPreviousExpand);

        (await RunAsync(expand, [[1, 2], [4]], column: 1)).Should().Equal(null, 1d, 2d);
    }

    [Fact]
    public async Task Expand_A_Later_Stage_Keeping_Its_Row_Sees_Each_Row_On_Both_Paths()
    {
        string[] stages = ["return [{ ...row, D: null }];", KeepsPreviousExpand];

        var rows = await ExpandAsync(stages);
        new[] { 1d, 2, 4 }.Select(a => rows.TransformMany([a]).Single()[1]).Should().Equal(null, 1d, 2d);

        _js.GetEngine().Evaluate("globalThis.__p = undefined;");
        var batches = await ExpandAsync(stages);
        (await RunAsync(batches, [[1, 2], [4]], column: 1)).Should().Equal(null, 1d, 2d);
    }

    // ── Window ───────────────────────────────────────────────────────────────

    private const string KeepsPreviousWindow =
        "const p = globalThis.__p; globalThis.__p = rows[0]; return [{ A: p ? rows[0].A - p.A : -1 }];";

    [Fact]
    public async Task Window_A_Script_Keeping_A_Row_Sees_Each_Row_On_The_Row_Path()
    {
        var window = await WindowAsync(KeepsPreviousWindow, count: 1);

        window.SharesRowView.Should().BeFalse();
        new[] { 1d, 2, 4 }.Select(a => window.TransformMany([a]).Single()[0]).Should().Equal(-1d, 1d, 2d);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Window_A_Script_Keeping_A_Row_Sees_Each_Row_On_The_Arrow_Path(bool oneBatch)
    {
        var window = await WindowAsync(KeepsPreviousWindow, count: 1);

        double[][] batches = oneBatch ? [[1, 2, 4]] : [[1, 2], [4]];
        (await RunAsync(window, batches, column: 0)).Should().Equal(-1d, 1d, 2d);
    }

    [Fact]
    public async Task Window_Rows_Kept_In_An_Outer_Array_Stay_Themselves_On_Both_Paths()
    {
        const string script = "globalThis.__acc = (globalThis.__acc || []).concat(rows); return rows.map(r => ({ A: globalThis.__acc[0].A }));";

        var rows = await WindowAsync(script, count: 2);
        new[] { 1d, 2, 4, 8 }.SelectMany(a => rows.TransformMany([a])).Select(r => r[0]).Should().Equal(1d, 1d, 1d, 1d);

        _js.GetEngine().Evaluate("globalThis.__acc = undefined;");
        var batches = await WindowAsync(script, count: 2);
        (await RunAsync(batches, [[1, 2, 4], [8]], column: 0)).Should().Equal(1d, 1d, 1d, 1d);
    }

    /// <summary>A window's array is the script's for one call: what it adds does not reach the next window.</summary>
    [Fact]
    public async Task Window_A_Change_To_The_Array_Stays_In_Its_Window_On_Both_Paths()
    {
        const string script = "rows.push({ A: rows.reduce((s, r) => s + r.A, 0) }); return rows;";

        var rows = await WindowAsync(script, count: 2);
        rows.SharesRowView.Should().BeFalse();
        new[] { 1d, 2, 4, 8 }.SelectMany(a => rows.TransformMany([a])).Select(r => r[0]).Should().Equal(1d, 2d, 3d, 4d, 8d, 12d);

        var batches = await WindowAsync(script, count: 2);
        (await RunAsync(batches, [[1, 2, 4, 8]], column: 0)).Should().Equal(1d, 2d, 3d, 4d, 8d, 12d);
    }

    [Fact]
    public async Task Window_A_Keyed_Window_Keeping_A_Row_Sees_It_After_Its_Batch()
    {
        IReadOnlyList<PipeColumnInfo> columns = [new("K", typeof(string), false), new("A", typeof(double), false)];
        var window = new WindowDataTransformer(
            new WindowOptions { Key = "K", Script = "const p = globalThis.__p; globalThis.__p = rows[0]; return rows.map(r => ({ K: r.K, A: p ? p.A : -1 }));" },
            _js);
        await window.InitializeAsync(columns);

        var schema = ArrowSchemaFactory.Create(columns);
        var output = new List<object?>();
        using (var batch = new RecordBatch(schema,
            [new StringArray.Builder().AppendRange(["x", "x", "y", "z"]).Build(), new DoubleArray.Builder().AppendRange([1, 2, 4, 8]).Build()], 4))
        {
            using var result = await window.TransformBatchAsync(batch);
            output.AddRange(Read(result, 1));
        }
        await foreach (var flushed in window.FlushBatchAsync())
        {
            using (flushed)
                output.AddRange(Read(flushed, 1));
        }

        output.Should().Equal(-1d, -1d, 1d, 4d);
    }

    // ── A script that keeps nothing stays on the shared view ─────────────────

    [Fact]
    public async Task A_Script_That_Keeps_Nothing_Stays_On_The_Shared_View()
    {
        (await ComputeAsync("D:double:row.A * 1.2")).SharesRowView.Should().BeTrue();
        (await ComputeAsync("D:const a = row.A; if (a >= 7000) return 'C'; return 'A';")).SharesRowView.Should().BeTrue();
        (await FilterAsync("row.A > 5000 && row.A % 2 === 0")).SharesRowView.Should().BeTrue();
        (await ExpandAsync("return [row, { ...row, A: row.A * 10 }];")).SharesRowView.Should().BeTrue();
        (await WindowAsync("return rows.map(r => ({ ...r, A: r.A * 1.1 }));", count: 2)).SharesRowView.Should().BeTrue();
        (await WindowAsync("return [{ A: rows.reduce((s, r) => s + r.A, 0) / rows.length }];", count: 2)).SharesRowView.Should().BeTrue();
    }

    // ── Errors read the same on either view ──────────────────────────────────

    public static TheoryData<string, string, string> Failures => new()
    {
        { "compute", "return row.Missing;", "*Column 'Missing' not found in schema*" },
        { "filter", "return row.Missing > 0;", "*Column 'Missing' not found in schema*A filter reads its columns off 'row'*" },
        { "expand", "return [{ A: row.Missing }];", "*--expand could not evaluate*Column 'Missing' not found in schema*" },
        { "window", "return [{ A: rows[0].Missing }];", "*Error evaluating window script*Column 'Missing' not found in schema*" },
    };

    [Theory]
    [MemberData(nameof(Failures))]
    public async Task An_Unknown_Column_Fails_Alike_On_Either_View_And_Either_Path(string kind, string body, string message)
    {
        foreach (var keeps in new[] { false, true })
        {
            var script = keeps ? KeepFirst(kind) + body : body;
            var transformer = await CreateAsync(kind, script, _js);
            SharesRowView(transformer).Should().Be(!keeps);

            var onRows = () => RunRow(transformer, 1d);
            onRows.Should().Throw<InvalidOperationException>().WithMessage(message);

            var onBatches = async () => await RunAsync(transformer, [[1]], column: 0);
            (await onBatches.Should().ThrowAsync<InvalidOperationException>()).WithMessage(message);
        }
    }

    [Theory]
    [InlineData("compute")]
    [InlineData("filter")]
    [InlineData("expand")]
    [InlineData("window")]
    public async Task A_Timeout_And_A_Memory_Limit_Fail_Alike_On_Either_View(string kind)
    {
        using var limited = new LimitedJsEngineProvider();
        foreach (var keeps in new[] { false, true })
        {
            var prefix = keeps ? KeepFirst(kind) : "";

            var looping = await CreateAsync(kind, prefix + "while (true) {} return [];", limited);
            SharesRowView(looping).Should().Be(!keeps);
            var loop = async () => await RunAsync(looping, [[1]], column: 0);
            (await loop.Should().ThrowAsync<InvalidOperationException>()).WithInnerException<TimeoutException>();

            var growing = await CreateAsync(kind, prefix + "const a = []; while (true) a.push(new Array(100000).fill(1)); return [];", limited);
            SharesRowView(growing).Should().Be(!keeps);
            var grow = async () => await RunAsync(growing, [[1]], column: 0);
            (await grow.Should().ThrowAsync<InvalidOperationException>()).WithInnerException<MemoryLimitExceededException>();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static string KeepFirst(string kind) => kind == "window" ? "globalThis.__k = rows[0]; " : "globalThis.__k = row; ";

    private async Task<ComputeDataTransformer> ComputeAsync(string mapping)
        => (ComputeDataTransformer)await CreateAsync("compute", mapping, _js, mapping: true);

    private async Task<FilterDataTransformer> FilterAsync(string script)
        => (FilterDataTransformer)await CreateAsync("filter", script, _js);

    private Task<ExpandDataTransformer> ExpandAsync(params string[] stages)
        => ExpandAsync(stages, _js);

    private static async Task<ExpandDataTransformer> ExpandAsync(string[] stages, IJsEngineProvider js)
    {
        var expand = new ExpandDataTransformer(
            new ExpandOptions { Expand = stages, ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["D"] = "double" } },
            js);
        await expand.InitializeAsync(Columns);
        return expand;
    }

    private async Task<WindowDataTransformer> WindowAsync(string script, int count)
    {
        var window = new WindowDataTransformer(new WindowOptions { Count = count, Script = script }, _js);
        await window.InitializeAsync(Columns);
        return window;
    }

    private static async Task<BaseColumnarTransformer> CreateAsync(string kind, string script, IJsEngineProvider js, bool mapping = false)
    {
        BaseColumnarTransformer transformer = kind switch
        {
            "compute" => new ComputeDataTransformer(new ComputeOptions { Compute = [mapping ? script : $"D:double:{script}"] }, js),
            "filter" => new FilterDataTransformer(new FilterOptions { Filters = [script] }, js),
            "expand" => new ExpandDataTransformer(new ExpandOptions { Expand = [script] }, js),
            "window" => new WindowDataTransformer(new WindowOptions { Count = 1, Script = script }, js),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
        await transformer.InitializeAsync(Columns);
        return transformer;
    }

    private static bool SharesRowView(BaseColumnarTransformer transformer) => transformer switch
    {
        ComputeDataTransformer c => c.SharesRowView,
        FilterDataTransformer f => f.SharesRowView,
        ExpandDataTransformer e => e.SharesRowView,
        WindowDataTransformer w => w.SharesRowView,
        _ => throw new ArgumentOutOfRangeException(nameof(transformer)),
    };

    private static void RunRow(BaseColumnarTransformer transformer, double a)
    {
        if (transformer is IMultiRowTransformer multi)
            _ = multi.TransformMany([a]).ToList();
        else
            transformer.Transform([a]);
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
            var input = Batch(values, null);
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

    private static RecordBatch Batch(double[] values, Apache.Arrow.Memory.MemoryAllocator? allocator)
    {
        var builder = new DoubleArray.Builder();
        builder.AppendRange(values);
        return new RecordBatch(
            new Schema([new Field("A", DoubleType.Default, false)], null),
            [builder.Build(allocator)],
            values.Length);
    }
}
