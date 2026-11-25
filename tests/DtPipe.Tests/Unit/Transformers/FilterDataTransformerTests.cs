using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Services;
using AwesomeAssertions;
using Jint.Runtime;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// The filter evaluates JavaScript predicates in both columnar Arrow mode and row mode.
/// The two execution paths must always agree on which rows are kept or rejected.
/// </summary>
public class FilterDataTransformerTests : IDisposable
{
    private readonly IJsEngineProvider _jsEngineProvider = new JsEngineProvider();

    public void Dispose() => _jsEngineProvider.Dispose();

    private FilterDataTransformer Build(string expression) =>
        new(new FilterOptions { Filters = new[] { expression } }, _jsEngineProvider);

    private static RecordBatch IntBatch(string name, params int[] values)
    {
        var schema = new Schema.Builder().Field(f => f.Name(name).DataType(Int32Type.Default)).Build();
        var array = new Int32Array.Builder().AppendRange(values).Build();
        return new RecordBatch(schema, new IArrowArray[] { array }, values.Length);
    }

    private static RecordBatch TextBatch(string name, params string?[] values)
    {
        var schema = new Schema.Builder().Field(f => f.Name(name).DataType(StringType.Default)).Build();
        var builder = new StringArray.Builder();
        foreach (var value in values)
        {
            if (value is null) builder.AppendNull();
            else builder.Append(value);
        }
        return new RecordBatch(schema, new IArrowArray[] { builder.Build() }, values.Length);
    }

    private static int[] KeptValues(RecordBatch? batch) =>
        batch is null
            ? System.Array.Empty<int>()
            : Enumerable.Range(0, batch.Length).Select(i => ((Int32Array)batch.Column(0)).GetValue(i)!.Value).ToArray();

    private static string[] KeptTexts(RecordBatch? batch) =>
        batch is null
            ? System.Array.Empty<string>()
            : Enumerable.Range(0, batch.Length).Select(i => ((StringArray)batch.Column(0)).GetString(i)).ToArray();

    private static List<PipeColumnInfo> IntColumn(string name) => new() { new(name, typeof(int), true) };
    private static List<PipeColumnInfo> TextColumn(string name) => new() { new(name, typeof(string), true) };

    // ── The two paths must agree ───────────────────────────────────────────────

    /// <summary>
    /// A text column holds "96575", and 96575 > 500. The fast path used to compare the two as
    /// strings — '9' against '5' — and answered on 539 of 1000 rows where JavaScript kept them all.
    /// </summary>
    [Theory]
    [InlineData("row.Score > 500", "96575", true)]
    [InlineData("row.Score > 500", "4850", true)]
    [InlineData("row.Score > 500", "12", false)]
    [InlineData("row.Score >= 500", "500", true)]
    public async Task RowMode_TextColumn_ComparesAsJavaScriptDoes(string expression, string value, bool kept)
    {
        var transformer = Build(expression);
        await transformer.InitializeAsync(TextColumn("Score"));

        var result = transformer.Transform(new object?[] { value });

        (result is not null).Should().Be(kept);
    }

    /// <summary>
    /// ">=" and "<=" must not be read as ">" and "<" against a value of "= 500": that parsed as no
    /// number, fell through to an ordinal comparison, and matched no row at all.
    /// </summary>
    [Theory]
    [InlineData("row.Value >= 500", 500, true)]
    [InlineData("row.Value >= 500", 499, false)]
    [InlineData("row.Value <= 500", 500, true)]
    [InlineData("row.Value <= 500", 501, false)]
    [InlineData("row.Value > 500", 501, true)]
    [InlineData("row.Value < 500", 499, true)]
    [InlineData("row.Value == 500", 500, true)]
    [InlineData("row.Value != 500", 500, false)]
    public async Task ColumnarMode_EveryOperatorIsReadWhole(string expression, int value, bool kept)
    {
        var transformer = Build(expression);
        await transformer.InitializeAsync(IntColumn("Value"));
        transformer.CanProcessColumnar.Should().BeTrue("all filter predicates run in columnar mode");

        using var result = await transformer.TransformBatchAsync(IntBatch("Value", value));

        (result is not null && result.Length == 1).Should().Be(kept);
    }

    [Fact]
    public async Task ColumnarAndRowModes_SelectTheSameRows()
    {
        var values = new[] { 1, 250, 499, 500, 501, 99_999 };

        var columnar = Build("row.Value >= 500");
        await columnar.InitializeAsync(IntColumn("Value"));
        using var batch = await columnar.TransformBatchAsync(IntBatch("Value", values));
        var fromColumnar = Enumerable.Range(0, batch!.Length)
            .Select(i => ((Int32Array)batch.Column(0)).GetValue(i)!.Value).ToArray();

        var rowMode = Build("row.Value >= 500");
        await rowMode.InitializeAsync(IntColumn("Value"));
        var fromRows = values.Where(v => rowMode.Transform(new object?[] { v }) is not null).ToArray();

        fromColumnar.Should().Equal(fromRows);
        fromColumnar.Should().Equal(500, 501, 99_999);
    }

    // ── Columnar Jint coverage & agreement with row mode ───────────────────────

    [Theory]
    [InlineData("row.Name == 'Ada'")]
    [InlineData("row.Name != 'Ada'")]
    public async Task TextEquality_RunsInColumnarMode(string expression)
    {
        var transformer = Build(expression);
        await transformer.InitializeAsync(TextColumn("Name"));
        transformer.CanProcessColumnar.Should().BeTrue();
    }

    [Theory]
    [InlineData("row.Name > 'Ada'")]
    [InlineData("row.Score > 500")]
    [InlineData("row.Value == null")]
    [InlineData("row.Value == true")]
    [InlineData("row.Value == 'a' + 'b'")]
    [InlineData("row.Name.length > 0")]
    [InlineData("Value > 500")]
    public async Task ComplexAndAmbiguousPredicates_RunInColumnarJavaScriptMode(string expression)
    {
        var transformer = Build(expression);
        await transformer.InitializeAsync(
            expression.Contains("Name") ? TextColumn("Name")
            : expression.Contains("Score") ? TextColumn("Score")
            : IntColumn("Value"));

        transformer.CanProcessColumnar.Should().BeTrue("all filter predicates run in columnar mode");
    }

    [Fact]
    public async Task ColumnarFallback_TextComparisonAndNullPropertyAccess_MatchRowMode()
    {
        var textFilter = Build("row.Score > 500");
        await textFilter.InitializeAsync(TextColumn("Score"));

        var schema = new Schema.Builder().Field(f => f.Name("Score").DataType(StringType.Default)).Build();
        var sb = new StringArray.Builder();
        sb.Append("96575");
        sb.Append("12");
        using var inBatch = new RecordBatch(schema, new IArrowArray[] { sb.Build() }, 2);
        using var outBatch = await textFilter.TransformBatchAsync(inBatch);
        outBatch.Should().NotBeNull();
        outBatch!.Length.Should().Be(1);
        ((StringArray)outBatch.Column(0)).GetString(0).Should().Be("96575");
        (textFilter.Transform(["96575"]) is not null).Should().BeTrue();
        (textFilter.Transform(["12"]) is not null).Should().BeFalse();

        var nullPropFilter = Build("row.Name.length > 3");
        await nullPropFilter.InitializeAsync(TextColumn("Name"));

        var nameSchema = new Schema.Builder().Field(f => f.Name("Name").DataType(StringType.Default)).Build();
        var nb = new StringArray.Builder();
        nb.AppendNull();
        nb.Append("Grace");
        using var nameBatch = new RecordBatch(nameSchema, new IArrowArray[] { nb.Build() }, 2);
        using var nameOut = await nullPropFilter.TransformBatchAsync(nameBatch);
        nameOut.Should().NotBeNull();
        nameOut!.Length.Should().Be(1);
        ((StringArray)nameOut.Column(0)).GetString(0).Should().Be("Grace");
    }

    [Theory]
    [InlineData("(() => { const { a } = row.Name; return a === 1; })()")]
    [InlineData("Object.keys(row.Name).length > 0")]
    [InlineData("(() => { for (const x of row.Name) { } return true; })()")]
    [InlineData("[...row.Name].length > 0")]
    [InlineData("'a' in row.Name")]
    public async Task A_Null_Column_Used_As_An_Object_Is_No_Match_Whatever_The_Form(string expression)
    {
        var filter = Build(expression);
        await filter.InitializeAsync(TextColumn("Name"));

        filter.Transform([null]).Should().BeNull();
    }

    [Theory]
    [InlineData("row.Value > 10 && row.Value <= 50")]
    [InlineData("row.Value == 5 || row.Value === 20")]
    [InlineData("!(row.Value > 15)")]
    [InlineData("row.Value % 2 == 0")]
    [InlineData("row.Value !== 15")]
    public async Task CompoundAndModuloFilters_ColumnarAgreesWithRowMode(string expression)
    {
        var values = new[] { 4, 5, 10, 15, 20, 50, 51 };

        var columnar = Build(expression);
        await columnar.InitializeAsync(IntColumn("Value"));
        using var outBatch = await columnar.TransformBatchAsync(IntBatch("Value", values));
        var colVals = outBatch is null
            ? System.Array.Empty<int>()
            : Enumerable.Range(0, outBatch.Length).Select(i => ((Int32Array)outBatch.Column(0)).GetValue(i)!.Value).ToArray();

        var rowMode = Build(expression);
        await rowMode.InitializeAsync(IntColumn("Value"));
        var rowVals = values.Where(v => rowMode.Transform(new object?[] { v }) is not null).ToArray();

        colVals.Should().Equal(rowVals);
    }

    // ── A missing column is an error, not an empty result ──────────────────────

    /// <summary>
    /// The sibling transformers all fail on a column that is not in the schema. The filter caught
    /// the ReferenceError and returned "no match", so every row was dropped: a zero-byte file and
    /// exit 0 where --compute, --project, --rename and --window all refuse.
    /// </summary>
    [Fact]
    public async Task MissingColumn_Throws_RatherThanDroppingEveryRow()
    {
        var transformer = Build("row.NoSuchColumn > 1");
        await transformer.InitializeAsync(IntColumn("Value"));

        var act = () => transformer.Transform(new object?[] { 1 });

        act.Should().Throw<InvalidOperationException>().WithMessage("*NoSuchColumn*");
    }

    /// <summary>The Arrow path refuses a missing column as the row path does.</summary>
    [Fact]
    public async Task MissingColumn_Throws_OnTheArrowPath_Too()
    {
        var transformer = Build("row.NoSuchColumn > 1");
        await transformer.InitializeAsync(IntColumn("Value"));

        var act = async () => await transformer.TransformBatchAsync(IntBatch("Value", 1));

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*NoSuchColumn*");
    }

    // ── A script's error is not "no match" ─────────────────────────────────────

    /// <summary>
    /// A read on null is a TypeError the engine words in two ways (a property, a method), and the filter
    /// recognises it by that wording: this test is what breaks if a newer engine words it otherwise.
    /// </summary>
    [Theory]
    [InlineData("row.Name.length > 3")]
    [InlineData("row.Name.includes('ra')")]
    [InlineData("row.Name['length'] > 3")]
    [InlineData("row.Name[0] === 'G'")]
    public async Task ReadingAPropertyOfNull_IsNoMatch_OnBothPaths(string expression)
    {
        var onRows = Build(expression);
        await onRows.InitializeAsync(TextColumn("Name"));
        onRows.Transform(new object?[] { null }).Should().BeNull();
        onRows.Transform(new object?[] { "Grace" }).Should().NotBeNull();

        var onBatches = Build(expression);
        await onBatches.InitializeAsync(TextColumn("Name"));
        using var result = await onBatches.TransformBatchAsync(TextBatch("Name", null, "Grace", null));
        KeptTexts(result).Should().Equal("Grace");
    }

    /// <summary>
    /// A kept text cell leaves as the bytes it came in: accents, a character outside the basic plane, kanji,
    /// an empty string and a null survive the compaction, whatever neighbours were dropped around them.
    /// </summary>
    [Fact]
    public async Task KeptText_IsTheSameCell_OnBothPaths_WhateverIsDroppedAroundIt()
    {
        string?[] texts = ["café", "dropped", null, "", "naïve 😀 日本語", "dropped too", "Ünï", "😀"];
        const string Expression = "row.Name === null || !row.Name.startsWith('dropped')";

        var onRows = Build(Expression);
        await onRows.InitializeAsync(TextColumn("Name"));
        var viaRows = texts.Select(t => onRows.Transform(new object?[] { t })).Where(r => r is not null).Select(r => (string?)r![0]).ToArray();

        var onBatches = Build(Expression);
        await onBatches.InitializeAsync(TextColumn("Name"));
        using var result = await onBatches.TransformBatchAsync(TextBatch("Name", texts));
        var text = (StringArray)result!.Column(0);
        var viaBatches = Enumerable.Range(0, text.Length).Select(i => text.IsNull(i) ? null : text.GetString(i)).ToArray();

        viaBatches.Should().Equal("café", null, "", "naïve 😀 日本語", "Ünï", "😀");
        viaBatches.Should().Equal(viaRows);
    }

    [Theory]
    [InlineData("row.Value.nope.deeper > 1")]
    [InlineData("({}).p.q === 1")]
    public async Task ReadingAPropertyOfUndefined_IsNoMatch_OnBothPaths(string expression)
    {
        var onRows = Build(expression);
        await onRows.InitializeAsync(IntColumn("Value"));
        onRows.Transform(new object?[] { 1 }).Should().BeNull();

        var onBatches = Build(expression);
        await onBatches.InitializeAsync(IntColumn("Value"));
        using var result = await onBatches.TransformBatchAsync(IntBatch("Value", 1, 2));
        result.Should().BeNull();
    }

    /// <summary>
    /// Any other error of the script fails the run and names the filter: a throw, a call of something
    /// that is not a function, an error of the engine. Read as "no match", each of them dropped every
    /// row without a word, in exit 0.
    /// </summary>
    [Theory]
    [InlineData("throw new Error('boom');", "boom")]
    [InlineData("return row.Value.nope();", "nope")]
    [InlineData("return JSON.parse('{');", "JSON")]
    [InlineData("return new Array(-1).length > 0;", "Invalid array length")]
    public async Task AnyOtherScriptError_FailsTheRun_OnBothPaths(string expression, string reason)
    {
        var onRows = Build(expression);
        await onRows.InitializeAsync(IntColumn("Value"));
        var rows = () => onRows.Transform(new object?[] { 1 });
        var rowFailure = rows.Should().Throw<InvalidOperationException>().Which;
        rowFailure.Message.Should().Contain($"Error evaluating filter '{expression}'").And.Contain(reason);

        var onBatches = Build(expression);
        await onBatches.InitializeAsync(IntColumn("Value"));
        var batches = async () => await onBatches.TransformBatchAsync(IntBatch("Value", 1));
        var batchFailure = (await batches.Should().ThrowAsync<InvalidOperationException>()).Which;
        batchFailure.Message.Should().Contain($"Error evaluating filter '{expression}'").And.Contain(reason);
    }

    [Theory]
    [InlineData("while (true) {} return true;", typeof(TimeoutException))]
    [InlineData("const a = []; while (true) a.push(new Array(100000).fill(1)); return true;", typeof(MemoryLimitExceededException))]
    public async Task ATimeoutAndAMemoryLimit_FailTheRun_OnBothPaths(string expression, Type limit)
    {
        using var limited = new LimitedJsEngineProvider();

        var onRows = new FilterDataTransformer(new FilterOptions { Filters = new[] { expression } }, limited);
        await onRows.InitializeAsync(IntColumn("Value"));
        var rows = () => onRows.Transform(new object?[] { 1 });
        var rowFailure = rows.Should().Throw<InvalidOperationException>().Which;
        rowFailure.Message.Should().Contain($"Error evaluating filter '{expression}'");
        rowFailure.InnerException.Should().BeOfType(limit);

        var onBatches = new FilterDataTransformer(new FilterOptions { Filters = new[] { expression } }, limited);
        await onBatches.InitializeAsync(IntColumn("Value"));
        var batches = async () => await onBatches.TransformBatchAsync(IntBatch("Value", 1));
        var batchFailure = (await batches.Should().ThrowAsync<InvalidOperationException>()).Which;
        batchFailure.Message.Should().Contain($"Error evaluating filter '{expression}'");
        batchFailure.InnerException.Should().BeOfType(limit);
    }

    // ── Stages read each other ─────────────────────────────────────────────────

    private FilterDataTransformer BuildStages(params string[] expressions) =>
        new(new FilterOptions { Filters = expressions }, _jsEngineProvider);

    /// <summary>A stage reads what the one before wrote to the row, on both paths, in a batch or a row at a time.</summary>
    [Fact]
    public async Task AStage_ReadsWhatTheStageBeforeWroteToTheRow_OnBothPaths()
    {
        string[] stages = ["row.x = row.A * 2; return true;", "return row.x > 3;"];

        var onRows = BuildStages(stages);
        await onRows.InitializeAsync(IntColumn("A"));
        new[] { 1, 2, 4 }.Where(a => onRows.Transform(new object?[] { a }) is not null).Should().Equal(2, 4);

        var onBatches = BuildStages(stages);
        await onBatches.InitializeAsync(IntColumn("A"));
        using var first = await onBatches.TransformBatchAsync(IntBatch("A", 1, 2, 4));
        using var second = await onBatches.TransformBatchAsync(IntBatch("A", 4, 1, 2, 1));
        KeptValues(first).Should().Equal(2, 4);
        KeptValues(second).Should().Equal(4, 2);
    }

    /// <summary>A row a stage does not keep reaches no later stage, so the later one never sees what it could not read.</summary>
    [Fact]
    public async Task ARowAStageDropsReachesNoLaterStage_OnBothPaths()
    {
        string[] stages = ["row.A > 1", "if (row.A < 2) throw new Error('reached'); return true;"];

        var onRows = BuildStages(stages);
        await onRows.InitializeAsync(IntColumn("A"));
        new[] { 1, 2, 4 }.Where(a => onRows.Transform(new object?[] { a }) is not null).Should().Equal(2, 4);

        var onBatches = BuildStages(stages);
        await onBatches.InitializeAsync(IntColumn("A"));
        using var result = await onBatches.TransformBatchAsync(IntBatch("A", 1, 2, 4));
        KeptValues(result).Should().Equal(2, 4);
    }

    // ── Row mode, JavaScript conditions ────────────────────────────────────────

    [Fact]
    public async Task Transform_ShouldKeepRow_WhenConditionIsTrue()
    {
        var transformer = Build("return row.Age > 18;");
        await transformer.InitializeAsync(new List<PipeColumnInfo> { new("Name", typeof(string), true), new("Age", typeof(int), false) });

        var row = new object?[] { "John", 25 };

        transformer.Transform(row).Should().BeEquivalentTo(row);
    }

    [Fact]
    public async Task Transform_ShouldDropRow_WhenConditionIsFalse()
    {
        var transformer = Build("return row.Age > 18;");
        await transformer.InitializeAsync(new List<PipeColumnInfo> { new("Name", typeof(string), true), new("Age", typeof(int), false) });

        transformer.Transform(new object?[] { "Kid", 10 }).Should().BeNull();
    }

    [Fact]
    public async Task Transform_ShouldApplyMultipleFilters()
    {
        var options = new FilterOptions { Filters = new[] { "row.A > 0", "row.B < 100" } };
        var transformer = new FilterDataTransformer(options, _jsEngineProvider);
        await transformer.InitializeAsync(new List<PipeColumnInfo> { new("A", typeof(int), false), new("B", typeof(int), false) });

        transformer.Transform(new object?[] { 10, 50 }).Should().NotBeNull();
        transformer.Transform(new object?[] { -1, 50 }).Should().BeNull();
        transformer.Transform(new object?[] { 10, 200 }).Should().BeNull();
    }
}
