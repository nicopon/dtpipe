using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Engine;
using DtPipe.Services;
using DtPipe.Tests.Helpers;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class JsColumnarAndFastPathTests : IDisposable
{
    private readonly JsEngineProvider _jsProvider = new();

    public void Dispose() => _jsProvider.Dispose();

    // ── Compute: columnar & parallel Jint ────────────────────────────────────

    [Fact]
    public async Task Compute_ColumnarArithmetic_MatchesRowModeAcrossTypesAndNulls()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), true),
            new("Amount", typeof(double), true),
            new("KeepUuid", typeof(Guid), false),
            new("PriceDec", typeof(decimal), false, Precision: 10, Scale: 2),
        };

        var options = new ComputeOptions
        {
            Compute =
            [
                "CalcDouble:double:row.Amount * 1.2 + 5",
                "CalcInt:int:Math.round(row.Id / 2)",
                "CalcStr:row.Amount * 2",
                "PriceDec:row.PriceDec * 1.1"
            ]
        };

        var colTransformer = new ComputeDataTransformer(options, _jsProvider);
        var outCols = await colTransformer.InitializeAsync(cols);
        colTransformer.CanProcessColumnar.Should().BeTrue();

        var rowTransformer = new ComputeDataTransformer(options, _jsProvider);
        await rowTransformer.InitializeAsync(cols);

        var uuid1 = Guid.NewGuid();
        var uuid2 = Guid.NewGuid();
        var inSchema = ArrowSchemaFactory.Create(cols);

        var idB = new Int32Array.Builder();
        idB.Append(3);
        idB.AppendNull();
        var amtB = new DoubleArray.Builder();
        amtB.Append(100.0);
        amtB.AppendNull();
        var uuidB = new FixedSizeBinaryArrayBuilder(16);
        uuidB.Append(ArrowTypeMapper.ToArrowUuidBytes(uuid1));
        uuidB.Append(ArrowTypeMapper.ToArrowUuidBytes(uuid2));
        var decB = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        decB.Append(10.00m);
        decB.Append(20.00m);

        using var inBatch = new RecordBatch(inSchema, [idB.Build(), amtB.Build(), uuidB.Build(), decB.Build()], 2);
        using var outBatch = await colTransformer.TransformBatchAsync(inBatch);
        outBatch.Should().NotBeNull();
        outBatch!.Length.Should().Be(2);

        outCols.Single(c => c.Name == "PriceDec").Precision.Should().BeNull();
        ArrowTypeMapper.GetValueForField(outBatch.Column(2), outBatch.Schema.GetFieldByIndex(2), 0).Should().Be(uuid1);
        ArrowTypeMapper.GetValueForField(outBatch.Column(2), outBatch.Schema.GetFieldByIndex(2), 1).Should().Be(uuid2);

        var r0 = rowTransformer.Transform([3, 100.0, uuid1, 10.00m])!;
        var r1 = rowTransformer.Transform([null, null, uuid2, 20.00m])!;

        for (int rowIdx = 0; rowIdx < 2; rowIdx++)
        {
            var expected = rowIdx == 0 ? r0 : r1;
            Convert.ToDouble(ArrowTypeMapper.GetValueForField(outBatch.Column(4), outBatch.Schema.GetFieldByIndex(4), rowIdx))
                .Should().BeApproximately(Convert.ToDouble(expected[4]), 1e-9);
            Convert.ToInt32(ArrowTypeMapper.GetValueForField(outBatch.Column(5), outBatch.Schema.GetFieldByIndex(5), rowIdx))
                .Should().Be(Convert.ToInt32(expected[5]));
            ArrowTypeMapper.GetValueForField(outBatch.Column(6), outBatch.Schema.GetFieldByIndex(6), rowIdx)?.ToString()
                .Should().Be(Convert.ToString(expected[6], System.Globalization.CultureInfo.InvariantCulture));
            Convert.ToDouble(ArrowTypeMapper.GetValueForField(outBatch.Column(3), outBatch.Schema.GetFieldByIndex(3), rowIdx))
                .Should().BeApproximately(Convert.ToDouble(expected[3]), 1e-9);
        }
    }

    [Fact]
    public async Task Compute_ColumnarArithmetic_SkipNullAndDivideByZeroBehavior()
    {
        var cols = new List<PipeColumnInfo> { new("Val", typeof(double), true) };
        var schema = ArrowSchemaFactory.Create(cols);

        var skipTrans = new ComputeDataTransformer(new ComputeOptions { Compute = ["Val:10 / row.Val"], SkipNull = true }, _jsProvider);
        await skipTrans.InitializeAsync(cols);

        var vb = new DoubleArray.Builder();
        vb.Append(2.0);
        vb.AppendNull();
        using var inBatch = new RecordBatch(schema, [vb.Build()], 2);
        using var outBatch = await skipTrans.TransformBatchAsync(inBatch);
        outBatch!.Column(0).IsNull(1).Should().BeTrue();
        ((DoubleArray)outBatch.Column(0)).Values[0].Should().Be(5.0);

        // Without SkipNull, null coerces to 0 -> 10 / 0 = Infinity -> throws InvalidOperationException.
        var strictTrans = new ComputeDataTransformer(new ComputeOptions { Compute = ["Val:10 / row.Val"], SkipNull = false }, _jsProvider);
        await strictTrans.InitializeAsync(cols);
        var act = async () => await strictTrans.TransformBatchAsync(inBatch);
        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*division by zero*");
    }

    [Fact]
    public async Task Compute_ColumnarString_MatchRowModeAndThrowOnUndefinedSplit()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("Email", typeof(string), true),
            new("First", typeof(string), true),
            new("Last", typeof(string), true),
        };
        var options = new ComputeOptions
        {
            Compute =
            [
                "Domain:row.Email.split('@')[1]",
                "Upper:row.First.toUpperCase()",
                "Trimmed:row.Last.trim()",
                "Sub:row.First.substring(0, 3)",
                "Full:row.First + ' ' + row.Last"
            ]
        };

        var t = new ComputeDataTransformer(options, _jsProvider);
        await t.InitializeAsync(cols);

        var schema = ArrowSchemaFactory.Create(cols);
        var eb = new StringArray.Builder(); eb.Append("a@corp.io"); eb.Append("b@org.net");
        var fb = new StringArray.Builder(); fb.Append("Alice"); fb.Append("Bob");
        var lb = new StringArray.Builder(); lb.Append("  Martin  "); lb.Append("Lee");
        using var inBatch = new RecordBatch(schema, [eb.Build(), fb.Build(), lb.Build()], 2);
        using var outBatch = await t.TransformBatchAsync(inBatch);

        ((StringArray)outBatch!.Column(3)).GetString(0).Should().Be("corp.io");
        ((StringArray)outBatch.Column(4)).GetString(0).Should().Be("ALICE");
        ((StringArray)outBatch.Column(5)).GetString(0).Should().Be("Martin");
        ((StringArray)outBatch.Column(6)).GetString(0).Should().Be("Ali");
        ((StringArray)outBatch.Column(6)).GetString(1).Should().Be("Bob");
        ((StringArray)outBatch.Column(7)).GetString(1).Should().Be("Bob Lee");

        // Out-of-bounds split index throws undefined error just like Jint.
        var badEb = new StringArray.Builder(); badEb.Append("no-at-sign");
        var badFb = new StringArray.Builder(); badFb.Append("X");
        var badLb = new StringArray.Builder(); badLb.Append("Y");
        using var badBatch = new RecordBatch(schema, [badEb.Build(), badFb.Build(), badLb.Build()], 1);
        var act = async () => await t.TransformBatchAsync(badBatch);
        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*returned undefined*");
    }

    [Fact]
    public async Task Compute_PureParallelAndImpureSequential_MatchRowModeOver2048Rows()
    {
        const int n = 2048;
        var cols = new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        var schema = ArrowSchemaFactory.Create(cols);
        var idB = new Int32Array.Builder();
        for (int i = 0; i < n; i++) idB.Append(i);
        using var inBatch = new RecordBatch(schema, [idB.Build()], n);

        // Pure multi-statement script -> runs parallel in TransformBatchAsync over 2048 rows.
        var pureTrans = new ComputeDataTransformer(
            new ComputeOptions { Compute = ["Band:const x = row.Id; if (x % 2 === 0) return 'E'; return 'O';"] },
            _jsProvider);
        await pureTrans.InitializeAsync(cols);

        using var pureOut = await pureTrans.TransformBatchAsync(inBatch);
        var bands = (StringArray)pureOut!.Column(1);
        for (int i = 0; i < n; i++)
            bands.GetString(i).Should().Be(i % 2 == 0 ? "E" : "O");

        // Impure script reading/mutating a top-level binding -> falls back to sequential Jint, producing
        // 0..n-1. A lexical binding, since the engine's global object is frozen (JsRealmFreeze).
        var engine = _jsProvider.GetEngine();
        engine.Evaluate("let __seq = 0;");
        var impureTrans = new ComputeDataTransformer(
            new ComputeOptions { Compute = ["Seq:int:__seq++"] },
            _jsProvider);
        await impureTrans.InitializeAsync(cols);

        using var impureOut = await impureTrans.TransformBatchAsync(inBatch);
        var seqs = (Int32Array)impureOut!.Column(1);
        for (int i = 0; i < n; i++)
            seqs.Values[i].Should().Be(i);
    }

    /// <summary>
    /// A script reads the row as it came in, whatever another script wrote to it: the row path and the
    /// Arrow path (sequential under 1024 rows, parallel above) give the same columns.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2048)]
    public async Task Compute_ScriptReadingAColumnAnotherRewrites_ReadsItAsItCame_OnEveryPath(int n)
    {
        var cols = new List<PipeColumnInfo> { new("A", typeof(double), false) };
        var options = new ComputeOptions { Compute = ["A:row.A * 10", "B:double:row.A + 1"] };

        var rowTransformer = new ComputeDataTransformer(options, _jsProvider);
        await rowTransformer.InitializeAsync(cols);
        var columnar = new ComputeDataTransformer(options, _jsProvider);
        await columnar.InitializeAsync(cols);

        // From 1: at 0, A * 10 + 1 and A + 1 agree and would hide the rewritten value.
        var values = new DoubleArray.Builder();
        for (int i = 1; i <= n; i++) values.Append(i);
        using var inBatch = new RecordBatch(ArrowSchemaFactory.Create(cols), [values.Build()], n);
        using var outBatch = await columnar.TransformBatchAsync(inBatch);
        var a = (DoubleArray)outBatch!.Column(0);
        var b = (DoubleArray)outBatch.Column(1);

        for (int i = 0; i < n; i++)
        {
            double input = i + 1;
            var row = rowTransformer.Transform([input])!;
            row[0].Should().Be(input * 10);
            row[1].Should().Be(input + 1);
            a.GetValue(i).Should().Be(input * 10);
            b.GetValue(i).Should().Be(input + 1);
        }
    }

    [Fact]
    public async Task Compute_ColumnarJint_PreservesExactJsSemantics_NullsAndUnicode()
    {
        const int n = 2048;
        var cols = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Amount", typeof(double), true),
            new("Txt", typeof(string), true),
        };
        var schema = ArrowSchemaFactory.Create(cols);

        var idB = new Int32Array.Builder();
        var amtB = new DoubleArray.Builder();
        var txtB = new StringArray.Builder();
        for (int i = 0; i < n; i++)
        {
            idB.Append(i);
            if (i % 4 == 0) amtB.AppendNull();
            else amtB.Append(i * 1.5);
            txtB.Append(i % 2 == 0 ? "ß" : "hello");
        }

        using var inBatch = new RecordBatch(schema, [idB.Build(), amtB.Build(), txtB.Build()], n);

        var options = new ComputeOptions
        {
            Compute =
            [
                "CondDouble:double:row.Id % 2 === 0 ? null : row.Amount * 2",
                "CopyAmount:double:row.Amount",
                "UpperTxt:row.Txt.toUpperCase()"
            ],
            SkipNull = false
        };

        var trans = new ComputeDataTransformer(options, _jsProvider);
        await trans.InitializeAsync(cols);

        using var outBatch = await trans.TransformBatchAsync(inBatch);
        outBatch.Should().NotBeNull();

        var condCol = (DoubleArray)outBatch!.Column(3);
        var copyCol = (DoubleArray)outBatch.Column(4);
        var upperCol = (StringArray)outBatch.Column(5);

        for (int i = 0; i < n; i++)
        {
            if (i % 2 == 0)
            {
                condCol.IsNull(i).Should().BeTrue();
                upperCol.GetString(i).Should().Be("SS");
            }
            else
            {
                condCol.IsNull(i).Should().BeFalse();
                condCol.Values[i].Should().BeApproximately((i * 1.5) * 2.0, 1e-9);
                upperCol.GetString(i).Should().Be("HELLO");
            }

            if (i % 4 == 0)
                copyCol.IsNull(i).Should().BeTrue();
            else
            {
                copyCol.IsNull(i).Should().BeFalse();
                copyCol.Values[i].Should().BeApproximately(i * 1.5, 1e-9);
            }
        }
    }

    [Fact]
    public async Task Compute_ColumnarMissingColumn_ThrowsInvalidOperationException()
    {
        var cols = new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        var t = new ComputeDataTransformer(
            new ComputeOptions { Compute = ["Out:const v = row.Missing; return v;"] },
            _jsProvider);
        await t.InitializeAsync(cols);

        using var batch = new RecordBatch(ArrowSchemaFactory.Create(cols), [new Int32Array.Builder().Append(1).Build()], 1);
        var act = async () => await t.TransformBatchAsync(batch);
        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*Missing*");
    }

    // ── Expand: structural unnest & columnar Jint ────────────────────────────

    [Fact]
    public async Task Expand_ColumnarUnnest_And_ChainedJint_WorkCorrectly()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Tags", typeof(List<string>), false),
        };
        var options = new ExpandOptions
        {
            Expand = ["row.Tags.map(t => ({ ...row, Tag: t }))"],
            ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["Tag"] = "string" }
        };

        var expand = new ExpandDataTransformer(options, _jsProvider);
        await expand.InitializeAsync(cols);
        expand.CanProcessColumnar.Should().BeTrue();
        expand.UsesNativeFastPath.Should().BeTrue();

        var valB = new StringArray.Builder();
        valB.Append("a"); valB.Append("b"); valB.Append("c");
        var offB = new ArrowBuffer.Builder<int>(4);
        offB.Append(0); offB.Append(2); offB.Append(2); offB.Append(3); // row1 has empty list
        var listType = new ListType(new Field("item", StringType.Default, true));
        var listArr = new ListArray(listType, 3, offB.Build(), valB.Build(), ArrowBuffer.Empty, 0, 0);
        var idArr = new Int32Array.Builder().AppendRange([10, 20, 30]).Build();
        var schema = new Schema([new Field("Id", Int32Type.Default, false), new Field("Tags", listType, false)], null);
        using var inBatch = new RecordBatch(schema, [idArr, listArr], 3);

        using var outBatch = await expand.TransformBatchAsync(inBatch);
        outBatch.Should().NotBeNull();
        outBatch!.Length.Should().Be(3);
        ((Int32Array)outBatch.Column(0)).Values.ToArray().Should().Equal(10, 10, 30);
        ((StringArray)outBatch.Column(2)).GetString(0).Should().Be("a");
        ((StringArray)outBatch.Column(2)).GetString(1).Should().Be("b");
        ((StringArray)outBatch.Column(2)).GetString(2).Should().Be("c");

        // JSON string parse expand -> columnar Jint fallback
        var jsonCols = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Tags", typeof(string), false) };
        var jsonExpand = new ExpandDataTransformer(
            new ExpandOptions { Expand = ["JSON.parse(row.Tags).map(t => ({ ...row, Tags: t }))"] },
            _jsProvider);
        await jsonExpand.InitializeAsync(jsonCols);
        jsonExpand.UsesNativeFastPath.Should().BeFalse();

        using var jsonIn = new RecordBatch(
            ArrowSchemaFactory.Create(jsonCols),
            [new Int32Array.Builder().Append(1).Build(), new StringArray.Builder().Append("[\"x\",\"y\"]").Build()],
            1);
        using var jsonOut = await jsonExpand.TransformBatchAsync(jsonIn);
        jsonOut!.Length.Should().Be(2);
        ((StringArray)jsonOut.Column(1)).GetString(0).Should().Be("x");
        ((StringArray)jsonOut.Column(1)).GetString(1).Should().Be("y");
    }

    /// <summary>
    /// The unnest hands a list's values over as they are only into a column of exactly their type: a decimal
    /// of another scale or a timestamp of another unit than the declared column goes through JavaScript, so
    /// the batch's schema describes the buffer it carries.
    /// </summary>
    [Fact]
    public async Task Expand_ListOfAnotherWidthThanTheDeclaredColumn_GivesAColumnItsSchemaDescribes()
    {
        var decimalType = new Decimal128Type(10, 2);
        using var decimals = await UnnestOneAsync(
            decimalType, new Decimal128Array.Builder(decimalType).Append(12.34m).Build(), "decimal");
        var decimalField = decimals.Schema.GetFieldByIndex(1);
        var decimalColumn = (Decimal128Array)decimals.Column(1);
        ((Decimal128Type)decimalColumn.Data.DataType).Scale.Should().Be(((Decimal128Type)decimalField.DataType).Scale);
        ((Decimal128Type)decimalColumn.Data.DataType).Precision.Should().Be(((Decimal128Type)decimalField.DataType).Precision);
        decimalColumn.GetValue(0).Should().Be(12.34m);

        var nanoseconds = new TimestampType(TimeUnit.Nanosecond, (string?)null);
        using var timestamps = await UnnestOneAsync(
            nanoseconds,
            new TimestampArray.Builder(nanoseconds).Append(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)).Build(),
            "datetime");
        var timestampField = (TimestampType)timestamps.Schema.GetFieldByIndex(1).DataType;
        ((TimestampType)timestamps.Column(1).Data.DataType).Unit.Should().Be(timestampField.Unit);
    }

    // Expands a one-row batch whose "Tags" list holds `values` into a declared column "V".
    private async Task<RecordBatch> UnnestOneAsync(IArrowType elementType, IArrowArray values, string declared)
    {
        var listType = new ListType(new Field("item", elementType, true));
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(values.Length).Build();
        var list = new ListArray(listType, 1, offsets, values, ArrowBuffer.Empty, 0, 0);
        using var batch = new RecordBatch(new Schema([new Field("Tags", listType, false)], null), [list], 1);

        var expand = new ExpandDataTransformer(
            new ExpandOptions
            {
                Expand = ["row.Tags.map(t => ({ ...row, V: t }))"],
                ExpandTypes = new(StringComparer.OrdinalIgnoreCase) { ["V"] = declared }
            },
            _jsProvider);
        await expand.InitializeAsync([new PipeColumnInfo("Tags", typeof(List<object>), false)]);
        expand.UsesNativeFastPath.Should().BeTrue();

        var result = await expand.TransformBatchAsync(batch);
        result.Should().NotBeNull();
        result!.Length.Should().Be(values.Length);
        return result;
    }

    /// <summary>
    /// Lists that cover a stretch of their values array hand that stretch over as a view of the same buffers: the
    /// input batch is disposed before the output is read, and the allocator ends at zero.
    /// </summary>
    [Fact]
    public async Task Expand_ListsOverAStretchOfText_UnnestsAViewThatOutlivesTheInput()
    {
        var pool = new DtPipe.Tests.Helpers.TrackingMemoryPool();
        var items = new StringArray.Builder();
        foreach (var item in new[] { "skipped", "é", "日本", "😀", "", "y", "skipped too" }) items.Append(item);

        // Row 0 covers items 1-2, row 1 none, row 2 items 3-5: the stretch starts at 1 and ends before the last value.
        var offsets = new ArrowBuffer.Builder<int>().Append(1).Append(3).Append(3).Append(6).Build(pool);
        var listType = new ListType(new Field("item", StringType.Default, true));
        var list = new ListArray(listType, 3, offsets, items.Build(pool), ArrowBuffer.Empty, 0, 0);
        var ids = new Int32Array.Builder().AppendRange([10, 20, 30]).Build(pool);
        var inBatch = new RecordBatch(
            new Schema([new Field("Id", Int32Type.Default, false), new Field("Tags", listType, false)], null), [ids, list], 3);

        var expand = new ExpandDataTransformer(
            new ExpandOptions { Expand = ["row.Tags.map(t => ({ ...row, Tag: t }))"], ExpandTypes = new() { ["Tag"] = "string" } },
            _jsProvider);
        await expand.InitializeAsync([new PipeColumnInfo("Id", typeof(int), false), new PipeColumnInfo("Tags", typeof(List<string>), false)]);
        expand.UsesNativeFastPath.Should().BeTrue();

        var outBatch = await expand.TransformBatchAsync(inBatch);
        inBatch.Dispose();

        outBatch.Should().NotBeNull();
        var tags = (StringArray)outBatch!.Column(2);
        Enumerable.Range(0, tags.Length).Select(i => tags.GetString(i)).Should().Equal("é", "日本", "😀", "", "y");
        ((Int32Array)outBatch.Column(0)).Values.ToArray().Should().Equal(10, 10, 30, 30, 30);

        outBatch.Dispose();
        pool.ActiveAllocations.Should().Be(0);
    }

    // ── Window: cross-batch count, key, window-all & mutations ───────────────

    [Fact]
    public async Task Window_ColumnarCrossBatch_CountKeyAndMutation_MatchRowMode()
    {
        var cols = new List<PipeColumnInfo>
        {
            new("Cat", typeof(string), false),
            new("Val", typeof(int), false)
        };
        var schema = ArrowSchemaFactory.Create(cols);

        // 1. Count = 3 across two batches of 2 rows each (total 4 rows -> 1 full + 1 flushed).
        var winCount = new WindowDataTransformer(
            new WindowOptions { Count = 3, Script = "return [{ Cat: rows[0].Cat, Val: rows.reduce((s, r) => s + r.Val, 0) }];" },
            _jsProvider);
        await winCount.InitializeAsync(cols);

        using var b1 = new RecordBatch(schema, [BuildStr("A", "A"), BuildInt(1, 2)], 2);
        using var b2 = new RecordBatch(schema, [BuildStr("A", "B"), BuildInt(3, 4)], 2);

        var out1 = await winCount.TransformBatchAsync(b1);
        out1.Should().BeNull();
        using var out2 = await winCount.TransformBatchAsync(b2);
        out2.Should().NotBeNull();
        out2!.Length.Should().Be(1);
        ((Int32Array)out2.Column(1)).Values[0].Should().Be(6);

        var flushed = new List<RecordBatch>();
        await foreach (var fb in winCount.FlushBatchAsync()) flushed.Add(fb);
        flushed.Should().ContainSingle();
        ((Int32Array)flushed[0].Column(1)).Values[0].Should().Be(4);
        foreach (var fb in flushed) fb.Dispose();

        // 2. Key-based across batch boundary + in-place mutation `r.Val = ...; return rows;`
        var winKey = new WindowDataTransformer(
            new WindowOptions { Key = "Cat", Script = "const sum = rows.reduce((s, r) => s + r.Val, 0); rows.forEach(r => r.Val = sum); return rows;" },
            _jsProvider);
        await winKey.InitializeAsync(cols);

        using var kb1 = new RecordBatch(schema, [BuildStr("X", "X"), BuildInt(10, 20)], 2);
        using var kb2 = new RecordBatch(schema, [BuildStr("X", "Y"), BuildInt(5, 99)], 2);

        (await winKey.TransformBatchAsync(kb1)).Should().BeNull("group X crosses batch boundary");
        using var kOut2 = await winKey.TransformBatchAsync(kb2);
        kOut2.Should().NotBeNull();
        kOut2!.Length.Should().Be(3);
        ((Int32Array)kOut2.Column(1)).Values.ToArray().Should().Equal(35, 35, 35);

        var kFlushed = new List<RecordBatch>();
        await foreach (var fb in winKey.FlushBatchAsync()) kFlushed.Add(fb);
        kFlushed.Should().ContainSingle();
        ((Int32Array)kFlushed[0].Column(1)).Values.ToArray().Should().Equal(99);
        foreach (var fb in kFlushed) fb.Dispose();
    }

    // ── Ownership: TrackingMemoryPool returns to zero across all 4 ────────────

    [Fact]
    public async Task AllFourColumnarTransformers_ReturnTrackingMemoryPoolToZero()
    {
        var pool = new TrackingMemoryPool();
        var executor = new PipelineExecutor(
            Enumerable.Empty<IRowToColumnarBridgeFactory>(),
            Enumerable.Empty<IColumnarToRowBridgeFactory>(),
            NullLogger<PipelineExecutor>.Instance);

        var listType = new ListType(new Field("item", StringType.Default, true));
        var uuidMeta = new Dictionary<string, string> { ["ARROW:extension:name"] = "arrow.uuid" };
        var schema = new Schema(
            [
                new Field("Id", Int32Type.Default, false),
                new Field("Uid", new FixedSizeBinaryType(16), false, uuidMeta),
                new Field("Amount", DoubleType.Default, false),
                new Field("Tags", listType, false),
            ],
            null);

        var cols = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Uid", typeof(Guid), false),
            new("Amount", typeof(double), false),
            new("Tags", typeof(List<string>), false),
        };

        var filter = new FilterDataTransformer(new FilterOptions { Filters = ["row.Amount > 10 && row.Id % 2 === 0"] }, _jsProvider);
        var compute = new ComputeDataTransformer(new ComputeOptions { Compute = ["Total:double:row.Amount * 2"] }, _jsProvider);
        var expand = new ExpandDataTransformer(
            new ExpandOptions { Expand = ["row.Tags.map(t => ({ ...row, Tag: t }))"], ExpandTypes = new() { ["Tag"] = "string" } },
            _jsProvider);
        var window = new WindowDataTransformer(new WindowOptions { Count = 2, Script = "rows" }, _jsProvider);

        var pipeline = new List<IDataTransformer> { filter, compute, expand, window };
        IReadOnlyList<PipeColumnInfo> curCols = cols;
        foreach (var t in pipeline)
            curCols = await t.InitializeAsync(curCols);

        async IAsyncEnumerable<RecordBatch> Source()
        {
            var idB = new Int32Array.Builder().AppendRange([1, 2, 3, 4]);
            var uidB = new FixedSizeBinaryArrayBuilder(16);
            for (int i = 0; i < 4; i++) uidB.Append(ArrowTypeMapper.ToArrowUuidBytes(Guid.NewGuid()));
            var amtB = new DoubleArray.Builder().AppendRange([5.0, 20.0, 30.0, 40.0]);
            var valB = new StringArray.Builder();
            var offB = new ArrowBuffer.Builder<int>(5);
            offB.Append(0);
            for (int i = 0; i < 4; i++)
            {
                valB.Append("t1"); valB.Append("t2");
                offB.Append((i + 1) * 2);
            }
            var listArr = new ListArray(listType, 4, offB.Build(pool), valB.Build(pool), ArrowBuffer.Empty, 0, 0);
            yield return new RecordBatch(schema, [idB.Build(pool), uidB.Build(), amtB.Build(pool), listArr], 4);
            await Task.Yield();
        }

        int totalRows = 0;
        await foreach (var outBatch in executor.ApplyColumnarSegmentAsync(Source(), pipeline, Mock.Of<IExportProgress>(), default))
        {
            totalRows += outBatch.Length;
            outBatch.Dispose();
        }

        totalRows.Should().Be(4); // rows Id=2 and Id=4 survive filter -> 2 tags each = 4 rows
        pool.ActiveAllocations.Should().Be(0);
    }

    private static StringArray BuildStr(params string[] vals)
    {
        var b = new StringArray.Builder();
        foreach (var v in vals) b.Append(v);
        return b.Build();
    }

    private static Int32Array BuildInt(params int[] vals) => new Int32Array.Builder().AppendRange(vals).Build();
}
