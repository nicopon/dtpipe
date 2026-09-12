using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Services;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// The filter carries a vectorised fast path beside the Jint one. It is an optimisation, so the
/// property these tests hold is that the two agree — a filter must not return a different set of
/// rows because the engine took a different route to it.
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
        transformer.CanProcessColumnar.Should().BeTrue("a numeric column against a numeric literal is the fast path");

        var result = await transformer.TransformBatchAsync(IntBatch("Value", value));

        (result is not null && result.Length == 1).Should().Be(kept);
    }

    [Fact]
    public async Task ColumnarAndRowModes_SelectTheSameRows()
    {
        var values = new[] { 1, 250, 499, 500, 501, 99_999 };

        var columnar = Build("row.Value >= 500");
        await columnar.InitializeAsync(IntColumn("Value"));
        var batch = await columnar.TransformBatchAsync(IntBatch("Value", values));
        var fromColumnar = Enumerable.Range(0, batch!.Length)
            .Select(i => ((Int32Array)batch.Column(0)).GetValue(i)!.Value).ToArray();

        var rowMode = Build("row.Value >= 500");
        await rowMode.InitializeAsync(IntColumn("Value"));
        var fromRows = values.Where(v => rowMode.Transform(new object?[] { v }) is not null).ToArray();

        fromColumnar.Should().Equal(fromRows);
        fromColumnar.Should().Equal(500, 501, 99_999);
    }

    // ── Eligibility: the fast path may decline, never disagree ─────────────────

    [Theory]
    [InlineData("row.Name == 'Ada'")]          // text column, quoted literal, equality
    [InlineData("row.Name != 'Ada'")]
    public async Task TextEquality_TakesTheFastPath(string expression)
    {
        var transformer = Build(expression);
        await transformer.InitializeAsync(TextColumn("Name"));
        transformer.CanProcessColumnar.Should().BeTrue();
    }

    [Theory]
    [InlineData("row.Name > 'Ada'")]           // quoted literal on a relational operator
    [InlineData("row.Score > 500")]            // text column against a number
    [InlineData("row.Value == null")]          // null literal
    [InlineData("row.Value == true")]          // boolean literal
    [InlineData("row.Value == 'a' + 'b'")]     // starts and ends with a quote, but is an expression
    [InlineData("row.Name.length > 0")]        // not a bare column reference
    [InlineData("Value > 500")]                // the undocumented spelling is no longer a trigger
    public async Task WhatCannotBeProvenEquivalent_FallsBackToJavaScript(string expression)
    {
        var transformer = Build(expression);
        await transformer.InitializeAsync(
            expression.Contains("Name") ? TextColumn("Name")
            : expression.Contains("Score") ? TextColumn("Score")
            : IntColumn("Value"));

        transformer.CanProcessColumnar.Should().BeFalse();
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

    /// <summary>A property read on a null value stays permissive — the distinction the throw is for.</summary>
    [Fact]
    public async Task PropertyOnNull_StillReadsAsNoMatch()
    {
        var transformer = Build("row.Name.length > 3");
        await transformer.InitializeAsync(TextColumn("Name"));

        transformer.Transform(new object?[] { null }).Should().BeNull();
        transformer.Transform(new object?[] { "Grace" }).Should().NotBeNull();
    }
}
