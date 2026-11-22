using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Services;
using AwesomeAssertions;
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

    /// <summary>A property read on a null value stays permissive — the distinction the throw is for.</summary>
    [Fact]
    public async Task PropertyOnNull_StillReadsAsNoMatch()
    {
        var transformer = Build("row.Name.length > 3");
        await transformer.InitializeAsync(TextColumn("Name"));

        transformer.Transform(new object?[] { null }).Should().BeNull();
        transformer.Transform(new object?[] { "Grace" }).Should().NotBeNull();
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
