using System.Collections;
using System.Globalization;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Tests.Unit.Conformance;
using Xunit;

namespace Apache.Arrow.Serialization.Tests;

/// <summary>
/// A column read through <see cref="ArrowTypeMap.CompileValueReader"/> gives, cell by cell, what
/// <see cref="ArrowTypeMap.GetValue"/> gives: the same value of the same type, null for a null, the same
/// exception where it throws. The conformance table's catalogue holds one case per Arrow type, so a type
/// added there is read here too; the cases below add what only a field's metadata tells apart.
/// </summary>
public class CompileValueReaderTests
{
    private const string Extension = ArrowExtensionInfo.NameKey;
    private static readonly Guid Uuid = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");

    public static TheoryData<string> CatalogueCases()
    {
        var names = new TheoryData<string>();
        foreach (var @case in ArrowTypeCases.All) names.Add(@case.Name);
        return names;
    }

    [Theory]
    [MemberData(nameof(CatalogueCases))]
    public void A_Column_Reads_As_GetValue_Reads_It(string name)
    {
        var @case = ArrowTypeCases.Named(name);
        AssertSameReading(@case.Build(), @case.Field);
    }

    /// <summary>A slice starts its values and its validity at an offset that a reader must apply to both.</summary>
    [Theory]
    [MemberData(nameof(CatalogueCases))]
    public void A_Slice_Reads_As_GetValue_Reads_It(string name)
    {
        var @case = ArrowTypeCases.Named(name);
        var column = @case.Build();
        AssertSameReading(ArrowArrayFactory.Slice(column, 1, column.Length - 1), @case.Field);
    }

    private static readonly Dictionary<string, (Field Field, Func<IArrowArray> Build)> Metadata = new()
    {
        ["opaque int128 over variable binary"] = (
            Logical(BinaryType.Default, typeof(Int128)),
            () => new BinaryArray.Builder().Append(ArrowOpaqueCodec.Encode(Int128.MaxValue)).AppendNull().Append("abc"u8).Build()),
        ["opaque of a vendor the codec does not know"] = (
            F(new FixedSizeBinaryType(4), (Extension, "arrow.opaque"), (ArrowExtensionInfo.MetadataKey, "{\"type_name\":\"x\",\"vendor_name\":\"Other\"}")),
            () => Fixed(4, [1, 2, 3, 4])),
        ["opaque over int8"] = (
            Logical(Int8Type.Default, typeof(Int128)),
            () => new Int8Array.Builder().Append(3).AppendNull().Build()),
        ["uuid over variable binary"] = (
            F(BinaryType.Default, (Extension, "arrow.uuid")),
            () => new BinaryArray.Builder().Append(ArrowTypeMap.ToArrowUuidBytes(Uuid)).AppendNull().Build()),
        ["uuid over eight bytes"] = (
            F(new FixedSizeBinaryType(8), (Extension, "arrow.uuid")),
            () => Fixed(8, [1, 2, 3, 4, 5, 6, 7, 8])),
        ["bool8 over int16"] = (
            F(Int16Type.Default, (Extension, "arrow.bool8")),
            () => new Int16Array.Builder().Append(1).AppendNull().Build()),
        ["bool8 of zero and a negative"] = (
            F(Int8Type.Default, (Extension, "arrow.bool8")),
            () => new Int8Array.Builder().Append(0).Append(-1).AppendNull().Build()),
        ["an extension with an empty name"] = (
            F(new FixedSizeBinaryType(16), (Extension, "")),
            () => Fixed(16, ArrowTypeMap.ToArrowUuidBytes(Uuid))),
        ["metadata that names no extension"] = (
            F(Int32Type.Default, ("comment", "x")),
            () => new Int32Array.Builder().Append(7).AppendNull().Build()),
        ["undeclared decimal256"] = (
            new Field("v", new Decimal256Type(38, 18), true, ArrowTypeMap.DefaultDecimal.Metadata),
            () => new Decimal256Array.Builder(new Decimal256Type(38, 18)).Append(4.29m).AppendNull().Build()),
        ["a column with no null"] = (
            F(Int32Type.Default),
            () => new Int32Array.Builder().Append(1).Append(2).Build()),
    };

    public static TheoryData<string> MetadataCases()
    {
        var names = new TheoryData<string>();
        foreach (var name in Metadata.Keys) names.Add(name);
        return names;
    }

    [Theory]
    [MemberData(nameof(MetadataCases))]
    public void A_Field_Reads_As_GetValue_Reads_It(string name)
    {
        var (field, build) = Metadata[name];
        AssertSameReading(build(), field);
    }

    private static void AssertSameReading(IArrowArray array, Field field)
    {
        var reader = ArrowTypeMap.CompileValueReader(array, field);
        for (var i = 0; i < array.Length; i++)
        {
            var index = i;
            Assert.Equal(Describe(() => ArrowTypeMap.GetValue(array, index, field)), Describe(() => reader(index)));
        }
    }

    /// <summary>A reading as text that tells apart what Equals does not: a runtime type, a DateTime's kind, an array's bytes.</summary>
    private static string Describe(Func<object?> read)
    {
        try { return Describe(read()); }
        catch (Exception e) { return $"throws {e.GetType().Name}: {e.Message}"; }
    }

    private static string Describe(object? value) => value switch
    {
        null => "null",
        byte[] bytes => $"byte[] {Convert.ToHexString(bytes)}",
        DateTime dateTime => $"DateTime {dateTime.Ticks} {dateTime.Kind}",
        DateTimeOffset instant => $"DateTimeOffset {instant.Ticks} {instant.Offset}",
        string text => $"String {text}",
        IDictionary map => $"{map.GetType().Name} {{{string.Join(", ", map.Keys.Cast<object>().Select(k => $"{Describe(k)}: {Describe(map[k])}"))}}}",
        IEnumerable items => $"{items.GetType().Name} [{string.Join(", ", items.Cast<object?>().Select(Describe))}]",
        _ => $"{value.GetType().Name} {Convert.ToString(value, CultureInfo.InvariantCulture)}"
    };

    private static Field F(IArrowType type, params (string Key, string Value)[] metadata)
        => new("v", type, true, metadata.Length == 0 ? null : metadata.Select(m => KeyValuePair.Create(m.Key, m.Value)));

    /// <summary>The metadata a CLR type's logical form carries, over another storage.</summary>
    private static Field Logical(IArrowType storage, Type clrType)
        => new("v", storage, true, ArrowTypeMap.GetLogicalType(clrType).Metadata);

    private static IArrowArray Fixed(int width, byte[] value)
    {
        var builder = new Reflection.FixedSizeBinaryArrayBuilder(width);
        builder.Append(value);
        builder.AppendNull();
        return builder.Build();
    }
}
