using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Tests.Helpers;
using DtPipe.Tests.Unit.Conformance;
using DtPipe.Transformers.Services;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// Compacting and gathering a text column copies its UTF-8 bytes, and slicing any column is a view over
/// the same buffers. The text here is multi-byte on purpose (accents, a character outside the basic
/// plane, kanji) with an empty string and a null among it, so a wrong offset or length lands on
/// the middle of a character or on a neighbour.
/// </summary>
public class ArrowColumnOperationsTests
{
    private static readonly Field Text = new("v", StringType.Default, true);

    private static StringArray TextArray(MemoryAllocator? pool, params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (var value in values)
        {
            if (value is null) builder.AppendNull();
            else builder.Append(value);
        }
        return builder.Build(pool);
    }

    private static string?[] Cells(IArrowArray array)
    {
        var text = (StringArray)array;
        return Enumerable.Range(0, text.Length).Select(i => text.IsNull(i) ? null : text.GetString(i)).ToArray();
    }

    private static readonly string?[] Source = ["café", null, "", "naïve 😀 日本語", "plain", "😀", "Ünï"];

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Compact_KeepsEachSelectedTextCell_ByteForByte(bool sliced)
    {
        // The sliced input starts at offset 2, so the offsets it reads are not zero-based.
        var whole = TextArray(null, Source);
        var input = sliced ? (StringArray)ArrowArrayFactory.Slice(whole, 2, 5) : whole;
        bool[] mask = sliced
            ? [false, true, false, true, true]
            : [true, true, true, false, true, true, false];

        var result = ArrowColumnOperations.CompactArray(input, Text, mask, mask.Count(m => m));

        Cells(result).Should().Equal(sliced
            ? new string?[] { "naïve 😀 日本語", "😀", "Ünï" }
            : new string?[] { "café", null, "", "plain", "😀" });
        result.NullCount.Should().Be(sliced ? 0 : 1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GatherScalarArray_RepeatsEachTextCell_ByteForByte(bool sliced)
    {
        // Repeats 2, 0, 1, 3: a repeated cell, a dropped one, a null and a character outside the basic plane.
        int[] offsets = [0, 2, 2, 3, 6];
        var whole = TextArray(null, "x", "café", "日本語", null, "😀");
        var input = sliced ? (StringArray)ArrowArrayFactory.Slice(whole, 1, 4) : TextArray(null, "café", "日本語", null, "😀");

        var result = ArrowColumnOperations.GatherScalarArray(input, Text, offsets, 4, 6);

        Cells(result).Should().Equal("café", "café", null, "😀", "😀", "😀");
    }

    [Fact]
    public void GatherListArray_RepeatsEachListOfText_ByteForByte()
    {
        var items = TextArray(null, "é", "日本", "😀", "", null);
        var offsetBuilder = new ArrowBuffer.Builder<int>(4);
        foreach (var offset in new[] { 0, 2, 2, 5 }) offsetBuilder.Append(offset);
        var listType = new ListType(new Field("item", StringType.Default, true));
        var lists = new ListArray(listType, 3, offsetBuilder.Build(), items, ArrowBuffer.Empty, 0, 0);

        // Lists of 2, 0 and 3 elements: each yields as many rows as it has elements, each row carrying the list.
        var result = ArrowColumnOperations.GatherListArray(lists, lists.ValueOffsets, 3, 5);

        result.Length.Should().Be(5);
        result.ValueOffsets.ToArray().Should().Equal(0, 2, 4, 7, 10, 13);
        Cells(result.Values).Should().Equal(
            "é", "日本", "é", "日本",
            "😀", "", null, "😀", "", null, "😀", "", null);
    }

    [Fact]
    public void SliceArray_OfText_IsAViewThatOutlivesTheArrayItWasCutFrom()
    {
        var pool = new TrackingMemoryPool();
        var source = TextArray(pool, Source);

        var slice = ArrowColumnOperations.SliceArray(source, 2, 4);
        source.Dispose();

        pool.ActiveAllocations.Should().BeGreaterThan(0, "the view still holds the buffers");
        Cells(slice).Should().Equal("", "naïve 😀 日本語", "plain", "😀");

        slice.Dispose();
        pool.ActiveAllocations.Should().Be(0);
    }

    public static TheoryData<string> CatalogueCases()
    {
        var names = new TheoryData<string>();
        foreach (var @case in ArrowTypeCases.All) names.Add(@case.Name);
        return names;
    }

    /// <summary>
    /// A slice covers the cells it names, for every Arrow type the catalogue holds (the ones no builder or
    /// reader names included: a copy through a builder could not serve them), and stays readable once the
    /// array it was cut from is disposed.
    /// </summary>
    [Theory]
    [MemberData(nameof(CatalogueCases))]
    public void SliceArray_CoversTheCellsItNames_ForEveryType(string name)
    {
        var @case = ArrowTypeCases.Named(name);
        var source = @case.Build();
        bool readable = ArrowTypeMap.HasValueReading(source.Data.DataType);
        var expected = readable ? Describe(source, @case.Field) : [];
        var expectedNulls = Enumerable.Range(0, source.Length).Select(source.IsNull).ToArray();

        var whole = ArrowColumnOperations.SliceArray(source, 0, source.Length);
        var tail = ArrowColumnOperations.SliceArray(source, 1, source.Length - 1);
        source.Dispose();

        whole.Length.Should().Be(expectedNulls.Length);
        tail.Length.Should().Be(expectedNulls.Length - 1);
        Enumerable.Range(0, whole.Length).Select(whole.IsNull).Should().Equal(expectedNulls);
        Enumerable.Range(0, tail.Length).Select(tail.IsNull).Should().Equal(expectedNulls.Skip(1));
        if (readable)
        {
            Describe(whole, @case.Field).Should().Equal(expected);
            Describe(tail, @case.Field).Should().Equal(expected.Skip(1));
        }

        whole.Dispose();
        tail.Dispose();
    }

    private static readonly JsonSerializerOptions Json = new() { NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowNamedFloatingPointLiterals };

    private static string[] Describe(IArrowArray array, Field field)
    {
        var read = ArrowTypeMap.CompileValueReader(array, field);
        return Enumerable.Range(0, array.Length).Select(i =>
        {
            var value = read(i);
            try { return JsonSerializer.Serialize(value, Json); }
            catch (NotSupportedException) { return value?.ToString() ?? "null"; }
        }).ToArray();
    }
}
