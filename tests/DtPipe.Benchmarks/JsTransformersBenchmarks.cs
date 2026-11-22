using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Services;

namespace DtPipe.Benchmarks;

/// <summary>
/// Measures the production JS-backed transformers across Row mode and Columnar Arrow mode
/// (including intra-batch parallel Jint for Compute and structural ListArray unnest for Expand).
///
/// Run:
///   dotnet run -c Release --project tests/DtPipe.Benchmarks -- --filter "*JsTransformersBenchmarks*"
/// </summary>
[MemoryDiagnoser]
public class JsTransformersBenchmarks
{
    private const int RowCount = 2048;
    private const int WindowSize = 16;

    private static readonly string[] Countries = ["FR", "DE", "US", "JP", "BR", "IN", "ZA", "CA"];

    private IReadOnlyList<PipeColumnInfo> _narrowCols = null!;
    private IReadOnlyList<PipeColumnInfo> _wideCols = null!;
    private IReadOnlyList<PipeColumnInfo> _expandWideCols = null!;

    private object?[][] _narrowRows = null!;
    private object?[][] _wideRows = null!;
    private object?[][] _expandWideRows = null!;

    private RecordBatch _narrowBatch = null!;
    private RecordBatch _wideBatch = null!;
    private RecordBatch _expandWideBatch = null!;

    private JsEngineProvider _jsProvider = null!;

    private ComputeDataTransformer _computeNarrowArith = null!;
    private ComputeDataTransformer _computeWideArith = null!;
    private ComputeDataTransformer _computeWideString = null!;
    private ComputeDataTransformer _computeWideComplex = null!;
    private ComputeDataTransformer _computeWideImpure = null!;

    private FilterDataTransformer _filterCompound = null!;
    private FilterDataTransformer _filterJsFallback = null!;

    private ExpandDataTransformer _expandUnnest = null!;
    private ExpandDataTransformer _expandJsSpread = null!;

    private WindowDataTransformer _windowAgg = null!;
    private WindowDataTransformer _windowMapSpread = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        BuildDatasets();
        _jsProvider = new JsEngineProvider();
        _jsProvider.GetEngine().Evaluate("var __benchSeq = 0;");

        _computeNarrowArith = new ComputeDataTransformer(
            new ComputeOptions
            {
                Compute = ["total:row.amount * 1.2"],
                ComputeTypes = new(StringComparer.OrdinalIgnoreCase) { ["total"] = "double" }
            },
            _jsProvider);
        await _computeNarrowArith.InitializeAsync(_narrowCols);

        _computeWideArith = new ComputeDataTransformer(
            new ComputeOptions
            {
                Compute = ["total:row.amount * 1.2"],
                ComputeTypes = new(StringComparer.OrdinalIgnoreCase) { ["total"] = "double" }
            },
            _jsProvider);
        await _computeWideArith.InitializeAsync(_wideCols);

        _computeWideString = new ComputeDataTransformer(
            new ComputeOptions { Compute = ["domain:row.email.split('@')[1]"] },
            _jsProvider);
        await _computeWideString.InitializeAsync(_wideCols);

        _computeWideComplex = new ComputeDataTransformer(
            new ComputeOptions { Compute = ["band:const a = row.amount; if (a >= 7000) return 'C'; if (a >= 4000) return 'B'; return 'A';"] },
            _jsProvider);
        await _computeWideComplex.InitializeAsync(_wideCols);

        _computeWideImpure = new ComputeDataTransformer(
            new ComputeOptions
            {
                Compute = ["seq:__benchSeq++"],
                ComputeTypes = new(StringComparer.OrdinalIgnoreCase) { ["seq"] = "int32" }
            },
            _jsProvider);
        await _computeWideImpure.InitializeAsync(_wideCols);

        _filterCompound = new FilterDataTransformer(
            new FilterOptions { Filters = ["row.amount > 5000 && row.country === 'FR'"] },
            _jsProvider);
        await _filterCompound.InitializeAsync(_wideCols);

        _filterJsFallback = new FilterDataTransformer(
            new FilterOptions { Filters = ["row.amount > 5000 && row.country.startsWith('F')"] },
            _jsProvider);
        await _filterJsFallback.InitializeAsync(_wideCols);

        _expandUnnest = new ExpandDataTransformer(
            new ExpandOptions
            {
                Expand = ["row.tags.map(t => ({ ...row, tag: t }))"],
                ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["tag"] = "string" }
            },
            _jsProvider);
        await _expandUnnest.InitializeAsync(_expandWideCols);

        _expandJsSpread = new ExpandDataTransformer(
            new ExpandOptions
            {
                Expand = ["row.tags.map(t => ({ ...row, tag: t.toUpperCase() }))"],
                ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["tag"] = "string" }
            },
            _jsProvider);
        await _expandJsSpread.InitializeAsync(_expandWideCols);

        _windowAgg = new WindowDataTransformer(
            new WindowOptions { Count = WindowSize, Script = "return [ { id: rows[0].id, amount: rows.reduce((s, r) => s + r.amount, 0) / rows.length } ];" },
            _jsProvider);
        await _windowAgg.InitializeAsync(_wideCols);

        _windowMapSpread = new WindowDataTransformer(
            new WindowOptions { Count = WindowSize, Script = "rows.map(r => ({ ...r, amount: r.amount * 1.1 }))" },
            _jsProvider);
        await _windowMapSpread.InitializeAsync(_wideCols);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _narrowBatch.Dispose();
        _wideBatch.Dispose();
        _expandWideBatch.Dispose();
        _jsProvider.Dispose();
    }

    // ── Compute ──────────────────────────────────────────────────────────────

    [Benchmark]
    public int Compute_Row_Narrow_Arith()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            if (_computeNarrowArith.Transform(_narrowRows[i]) != null) count++;
        return count;
    }

    [Benchmark]
    public int Compute_Row_Wide_Arith()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            if (_computeWideArith.Transform(_wideRows[i]) != null) count++;
        return count;
    }

    [Benchmark]
    public int Compute_Row_Wide_String()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            if (_computeWideString.Transform(_wideRows[i]) != null) count++;
        return count;
    }

    [Benchmark]
    public int Compute_Row_Wide_Complex()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            if (_computeWideComplex.Transform(_wideRows[i]) != null) count++;
        return count;
    }

    [Benchmark]
    public async Task<int> Compute_Columnar_Wide_Arith_JintParallel()
    {
        using var outBatch = await _computeWideArith.TransformBatchAsync(_wideBatch);
        return outBatch!.Length;
    }

    [Benchmark]
    public async Task<int> Compute_Columnar_Wide_String_JintParallel()
    {
        using var outBatch = await _computeWideString.TransformBatchAsync(_wideBatch);
        return outBatch!.Length;
    }

    [Benchmark]
    public async Task<int> Compute_Columnar_Wide_Complex_JintParallel()
    {
        using var outBatch = await _computeWideComplex.TransformBatchAsync(_wideBatch);
        return outBatch!.Length;
    }

    [Benchmark]
    public async Task<int> Compute_Columnar_Wide_Impure_JintSeq()
    {
        using var outBatch = await _computeWideImpure.TransformBatchAsync(_wideBatch);
        return outBatch!.Length;
    }

    // ── Filter ───────────────────────────────────────────────────────────────

    [Benchmark]
    public int Filter_Row_Wide_Compound()
    {
        int kept = 0;
        for (int i = 0; i < RowCount; i++)
            if (_filterCompound.Transform(_wideRows[i]) != null) kept++;
        return kept;
    }

    [Benchmark]
    public async Task<int> Filter_Columnar_Wide_Compound()
    {
        using var outBatch = await _filterCompound.TransformBatchAsync(_wideBatch);
        return outBatch?.Length ?? 0;
    }

    [Benchmark]
    public async Task<int> Filter_Columnar_Wide_MethodCall()
    {
        using var outBatch = await _filterJsFallback.TransformBatchAsync(_wideBatch);
        return outBatch?.Length ?? 0;
    }

    // ── Expand ───────────────────────────────────────────────────────────────

    [Benchmark]
    public int Expand_Row_Wide()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            foreach (var _ in _expandUnnest.TransformMany(_expandWideRows[i])) count++;
        return count;
    }

    [Benchmark]
    public async Task<int> Expand_Columnar_Wide_NativeUnnest()
    {
        using var outBatch = await _expandUnnest.TransformBatchAsync(_expandWideBatch);
        return outBatch!.Length;
    }

    [Benchmark]
    public async Task<int> Expand_Columnar_Wide_JsFallback()
    {
        using var outBatch = await _expandJsSpread.TransformBatchAsync(_expandWideBatch);
        return outBatch!.Length;
    }

    // ── Window ───────────────────────────────────────────────────────────────

    [Benchmark]
    public int Window_Row_Wide_Agg()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            foreach (var _ in _windowAgg.TransformMany(_wideRows[i])) count++;
        foreach (var _ in _windowAgg.Flush()) count++;
        return count;
    }

    [Benchmark]
    public async Task<int> Window_Columnar_Wide_Agg()
    {
        using var outBatch = await _windowAgg.TransformBatchAsync(_wideBatch);
        int count = outBatch?.Length ?? 0;
        await foreach (var fb in _windowAgg.FlushBatchAsync())
        {
            count += fb.Length;
            fb.Dispose();
        }
        return count;
    }

    [Benchmark]
    public int Window_Row_Wide_MapSpread()
    {
        int count = 0;
        for (int i = 0; i < RowCount; i++)
            foreach (var _ in _windowMapSpread.TransformMany(_wideRows[i])) count++;
        foreach (var _ in _windowMapSpread.Flush()) count++;
        return count;
    }

    [Benchmark]
    public async Task<int> Window_Columnar_Wide_MapSpread()
    {
        using var outBatch = await _windowMapSpread.TransformBatchAsync(_wideBatch);
        int count = outBatch?.Length ?? 0;
        await foreach (var fb in _windowMapSpread.FlushBatchAsync())
        {
            count += fb.Length;
            fb.Dispose();
        }
        return count;
    }

    // ── Dataset setup ────────────────────────────────────────────────────────

    private void BuildDatasets()
    {
        var narrowList = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), false),
            new("email", typeof(string), false),
            new("amount", typeof(double), false),
            new("country", typeof(string), false),
        };
        _narrowCols = narrowList;

        var wideList = new List<PipeColumnInfo>(narrowList);
        for (int i = 0; i < 9; i++) wideList.Add(new($"extra_i_{i}", typeof(int), false));
        for (int i = 0; i < 8; i++) wideList.Add(new($"extra_d_{i}", typeof(double), false));
        for (int i = 0; i < 8; i++) wideList.Add(new($"extra_s_{i}", typeof(string), false));
        _wideCols = wideList;

        _expandWideCols = [.. wideList, new PipeColumnInfo("tags", typeof(List<string>), false)];

        var ids = new int[RowCount];
        var names = new string[RowCount];
        var emails = new string[RowCount];
        var amounts = new double[RowCount];
        var countries = new string[RowCount];

        var extraInts = new int[9][];
        for (int c = 0; c < 9; c++) extraInts[c] = new int[RowCount];
        var extraDoubles = new double[8][];
        for (int c = 0; c < 8; c++) extraDoubles[c] = new double[RowCount];
        var extraStrings = new string[8][];
        for (int c = 0; c < 8; c++) extraStrings[c] = new string[RowCount];

        var rng = new Random(42);
        _narrowRows = new object?[RowCount][];
        _wideRows = new object?[RowCount][];
        _expandWideRows = new object?[RowCount][];

        for (int i = 0; i < RowCount; i++)
        {
            ids[i] = i + 1;
            names[i] = $"Firstname{i} Lastname{i}";
            emails[i] = $"user{i}.sample@example.com";
            amounts[i] = 100.0 + ((i * 13) % 10000);
            countries[i] = Countries[i % Countries.Length];

            _narrowRows[i] = [ids[i], names[i], emails[i], amounts[i], countries[i]];

            var wideRow = new object?[30];
            wideRow[0] = ids[i];
            wideRow[1] = names[i];
            wideRow[2] = emails[i];
            wideRow[3] = amounts[i];
            wideRow[4] = countries[i];

            int pos = 5;
            for (int c = 0; c < 9; c++) { int v = rng.Next(1, 10000); extraInts[c][i] = v; wideRow[pos++] = v; }
            for (int c = 0; c < 8; c++) { double v = Math.Round(rng.NextDouble() * 1000.0, 2); extraDoubles[c][i] = v; wideRow[pos++] = v; }
            for (int c = 0; c < 8; c++) { string v = $"val_{c}_{i & 63}"; extraStrings[c][i] = v; wideRow[pos++] = v; }
            _wideRows[i] = wideRow;

            var expRow = new object?[31];
            System.Array.Copy(wideRow, expRow, 30);
            expRow[30] = new List<object?> { "t1", "t2", "t3" };
            _expandWideRows[i] = expRow;
        }

        _narrowBatch = new RecordBatch(
            ArrowSchemaFactory.Create(_narrowCols),
            [
                BuildInt32Array(ids),
                BuildStringArray(names),
                BuildStringArray(emails),
                BuildDoubleArray(amounts),
                BuildStringArray(countries),
            ],
            RowCount);

        var wideArrays = new IArrowArray[30];
        for (int i = 0; i < 5; i++) wideArrays[i] = ArrowOwnership.RetainArray(_narrowBatch.Column(i));
        int arrIdx = 5;
        for (int c = 0; c < 9; c++) wideArrays[arrIdx++] = BuildInt32Array(extraInts[c]);
        for (int c = 0; c < 8; c++) wideArrays[arrIdx++] = BuildDoubleArray(extraDoubles[c]);
        for (int c = 0; c < 8; c++) wideArrays[arrIdx++] = BuildStringArray(extraStrings[c]);
        _wideBatch = new RecordBatch(ArrowSchemaFactory.Create(_wideCols), wideArrays, RowCount);

        var expandArrays = new IArrowArray[31];
        for (int i = 0; i < 30; i++) expandArrays[i] = ArrowOwnership.RetainArray(_wideBatch.Column(i));
        expandArrays[30] = BuildTagsListArray();
        _expandWideBatch = new RecordBatch(ArrowSchemaFactory.Create(_expandWideCols), expandArrays, RowCount);
    }

    private static IArrowArray BuildInt32Array(int[] values)
    {
        var b = new Int32Array.Builder();
        b.AppendRange(values);
        return b.Build();
    }

    private static IArrowArray BuildDoubleArray(double[] values)
    {
        var b = new DoubleArray.Builder();
        b.AppendRange(values);
        return b.Build();
    }

    private static IArrowArray BuildStringArray(string[] values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) b.Append(v);
        return b.Build();
    }

    private static ListArray BuildTagsListArray()
    {
        var valBuilder = new StringArray.Builder();
        var offBuilder = new ArrowBuffer.Builder<int>(RowCount + 1);
        int offset = 0;
        offBuilder.Append(0);
        for (int i = 0; i < RowCount; i++)
        {
            valBuilder.Append("t1"); valBuilder.Append("t2"); valBuilder.Append("t3");
            offset += 3;
            offBuilder.Append(offset);
        }
        var listType = new ListType(new Field("item", StringType.Default, true));
        return new ListArray(listType, RowCount, offBuilder.Build(), valBuilder.Build(), ArrowBuffer.Empty, 0, 0);
    }
}
