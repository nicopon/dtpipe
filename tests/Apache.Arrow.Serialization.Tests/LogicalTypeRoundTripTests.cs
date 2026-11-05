using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using AwesomeAssertions;
using Xunit;

namespace Apache.Arrow.Serialization.Tests;

/// <summary>
/// The map's two directions agree: a CLR type names a field, the field names the type back, and a
/// value of that type comes out of the dynamic serializer as itself.
/// </summary>
public class LogicalTypeRoundTripTests
{
    private static readonly Guid Uuid = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");

    public static TheoryData<Type, object> Values => new()
    {
        { typeof(bool), true },
        { typeof(sbyte), (sbyte)-5 },
        { typeof(short), (short)-300 },
        { typeof(int), -70000 },
        { typeof(long), -5000000000L },
        { typeof(byte), (byte)200 },
        { typeof(ushort), (ushort)60000 },
        { typeof(uint), 4000000000U },
        { typeof(ulong), 18000000000000000000UL },
        { typeof(float), 1.5f },
        { typeof(double), 1.5 },
        { typeof(decimal), 4.29m },
        { typeof(string), "héllo" },
        { typeof(byte[]), new byte[] { 1, 2, 3 } },
        { typeof(Guid), Uuid },
        { typeof(DateTime), new DateTime(2024, 1, 2, 3, 4, 5, 123, 456, DateTimeKind.Unspecified) },
        { typeof(DateTimeOffset), new DateTimeOffset(2024, 1, 2, 3, 4, 5, 123, 456, TimeSpan.Zero) },
        { typeof(DateOnly), new DateOnly(2024, 1, 2) },
        { typeof(TimeOnly), new TimeOnly(10, 11, 12, 123, 456) },
        { typeof(TimeSpan), TimeSpan.FromSeconds(90) },
        { typeof(Int128), Int128.MaxValue },
        { typeof(UInt128), UInt128.MaxValue },
    };

    [Theory]
    [MemberData(nameof(Values))]
    public void A_Field_Names_Back_The_Type_It_Was_Made_From(Type type, object _)
        => ArrowTypeMap.GetClrTypeFromField(ArrowTypeMap.GetField("v", ArrowTypeMap.GetLogicalType(type))).Should().Be(type);

    [Theory]
    [MemberData(nameof(Values))]
    public async Task A_Value_Comes_Back_Through_The_Dynamic_Serializer(Type type, object value)
    {
        var schema = new Schema([ArrowTypeMap.GetField("v", ArrowTypeMap.GetLogicalType(type))], null);

        using var batch = await ArrowSerializer.SerializeAsync(new[] { Row(value), Row(null) }, schema);

        var field = batch.Schema.GetFieldByIndex(0);
        var read = ArrowTypeMap.GetValue(batch.Column(0), 0, field);
        read.Should().BeOfType(type);
        read.Should().BeEquivalentTo(value);
        ArrowTypeMap.GetValue(batch.Column(0), 1, field).Should().BeNull();
    }

    public static TheoryData<IArrowType, object, object> OtherForms => new()
    {
        // The array a batch carries is the one its schema declares, and the map reads it back.
        { new Decimal128Type(10, 2), 4.2m, 4.20m },
        { new Decimal256Type(50, 10), 4.2m, 4.2000000000m },
        { new TimestampType(TimeUnit.Millisecond, "Asia/Tokyo"), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero) },
        { Date64Type.Default, new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified), new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified) },
        { new Time32Type(TimeUnit.Millisecond), new TimeOnly(10, 11, 12, 123), new TimeOnly(10, 11, 12, 123) },
        { new FixedSizeBinaryType(8), new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 } },
        // Either kind fits either timestamp field.
        { new TimestampType(TimeUnit.Microsecond, "UTC"), new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero) },
        { new TimestampType(TimeUnit.Microsecond, (string?)null), new DateTimeOffset(2024, 1, 2, 5, 4, 5, TimeSpan.FromHours(2)), new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified) },
    };

    [Theory]
    [MemberData(nameof(OtherForms))]
    public async Task The_Array_Is_Of_The_Type_The_Schema_Declares(IArrowType arrowType, object value, object expected)
    {
        var field = new Field("v", arrowType, true);

        using var batch = await ArrowSerializer.SerializeAsync(new[] { Row(value) }, new Schema([field], null));

        batch.Column(0).Data.DataType.Should().BeOfType(arrowType.GetType());
        ArrowTypeMap.GetValue(batch.Column(0), 0, field).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task A_Guid_In_A_Bare_Sixteen_Byte_Field_Writes_The_Bytes_Of_A_Uuid()
    {
        var field = new Field("v", new FixedSizeBinaryType(16), true);

        using var batch = await ArrowSerializer.SerializeAsync(new[] { Row(Uuid) }, new Schema([field], null));

        ArrowTypeMap.GetValue(batch.Column(0), 0, field).Should().BeEquivalentTo(ArrowTypeMap.ToArrowUuidBytes(Uuid));
    }

    [Fact]
    public async Task An_Element_Keeps_The_Meaning_Its_Field_Carries()
    {
        var uuidField = ArrowTypeMap.GetField("item", ArrowTypeMap.GetLogicalType(typeof(Guid)));
        var wallClockField = new Field("item", new TimestampType(TimeUnit.Microsecond, (string?)null), true);
        var clock = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        var schema = new Schema([new Field("u", new ListType(uuidField), true), new Field("t", new ListType(wallClockField), true)], null);

        using var batch = await ArrowSerializer.SerializeAsync(
            new[] { new Dictionary<string, object?> { ["u"] = new List<object?> { Uuid }, ["t"] = new List<object?> { clock } } }, schema);

        ArrowTypeMap.GetValue(batch.Column(0), 0, schema.GetFieldByIndex(0)).Should().BeEquivalentTo(new List<object?> { Uuid });
        ArrowTypeMap.GetValue(batch.Column(1), 0, schema.GetFieldByIndex(1)).Should().BeEquivalentTo(new List<object?> { clock });
    }

    private static Dictionary<string, object?> Row(object? value) => new() { ["v"] = value };
}
