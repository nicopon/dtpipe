using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// The Arrow shapes the C Data interface produces that the type map could not read: a
/// dictionary-encoded column, a fixed-size list, a canonical bool8, and the opaque payloads
/// DuckDB uses for types Arrow has no form for.
/// </summary>
/// <remarks>
/// Each of these made the SQL processor emit a wrong value or throw, against a row-mode reader
/// that read the same column correctly — so a regression here is two read paths disagreeing
/// again, not merely a narrower type map.
/// </remarks>
public class ArrowExtensionAndCompositeValueTests
{
	private static Field ExtensionField(string name, IArrowType type, string extensionName, string? metadata = null)
	{
		var meta = new Dictionary<string, string> { [ArrowExtensionInfo.NameKey] = extensionName };
		if (metadata != null) meta[ArrowExtensionInfo.MetadataKey] = metadata;
		return new Field(name, type, true, meta);
	}

	private static Field OpaqueField(string typeName, IArrowType type)
		=> ExtensionField("v", type, "arrow.opaque", $"{{\"type_name\":\"{typeName}\",\"vendor_name\":\"DuckDB\"}}");

	// ── Dictionary encoding (a DuckDB ENUM) ──────────────────────────────────────────

	[Fact]
	public void GetValue_ShouldResolveDictionaryEncodedValue()
	{
		var dictionary = new StringArray.Builder().Append("alpha").Append("beta").Build();
		var indices = new Int32Array.Builder().Append(1).Append(0).Build();
		var array = new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false), indices, dictionary);

		ArrowTypeMap.GetValue(array, 0).Should().Be("beta");
		ArrowTypeMap.GetValue(array, 1).Should().Be("alpha");
	}

	[Fact]
	public void GetClrType_ShouldReportTheValueType_ForADictionaryColumn()
	{
		// The encoding is storage. A consumer receives the decoded value, so the declared type
		// must be the value's, not a fallback to string for any dictionary whatsoever.
		var type = new DictionaryType(Int32Type.Default, Int64Type.Default, false);

		ArrowTypeMap.GetClrType(type).Should().Be<long>();
	}

	// ── Fixed-size list (a DuckDB ARRAY) ─────────────────────────────────────────────

	[Fact]
	public void GetValue_ShouldReadAFixedSizeList()
	{
		var values = new Int32Array.Builder().Append(1).Append(2).Append(3).Append(4).Build();
		var type = new FixedSizeListType(new Field("item", Int32Type.Default, true), 2);
		var array = new FixedSizeListArray(type, 2, values, new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Build());

		ArrowTypeMap.GetValue(array, 0).Should().BeEquivalentTo(new object?[] { 1, 2 });
		ArrowTypeMap.GetValue(array, 1).Should().BeEquivalentTo(new object?[] { 3, 4 });
	}

	// ── Canonical bool8 ──────────────────────────────────────────────────────────────

	[Theory]
	[InlineData((sbyte)1, true)]
	[InlineData((sbyte)0, false)]
	public void GetValue_ShouldReadABool8AsABoolean(sbyte stored, bool expected)
	{
		var array = new Int8Array.Builder().Append(stored).Build();
		var field = ExtensionField("v", Int8Type.Default, "arrow.bool8");

		ArrowTypeMap.GetValue(array, 0, field).Should().Be(expected);
		ArrowTypeMap.GetClrTypeFromField(field).Should().Be<bool>();
	}

	// ── Opaque payloads ──────────────────────────────────────────────────────────────

	// Byte layouts read off real DuckDB values; a decode guessed from the type name would
	// produce plausible wrong numbers rather than an error.
	[Theory]
	[InlineData("hugeint", new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, "1")]
	[InlineData("hugeint", new byte[] { 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, "256")]
	[InlineData("hugeint", new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, "-1")]
	[InlineData("hugeint", new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 127 }, "170141183460469231731687303715884105727")]
	[InlineData("hugeint", new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128 }, "-170141183460469231731687303715884105728")]
	[InlineData("uhugeint", new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, "340282366920938463463374607431768211455")]
	public void GetValue_ShouldDecodeAnOpaqueInteger(string typeName, byte[] payload, string expected)
	{
		var array = BuildFixedSizeBinary(payload);
		var field = OpaqueField(typeName, new FixedSizeBinaryType(payload.Length));

		ArrowTypeMap.GetValue(array, 0, field).Should().Be(expected);
		ArrowTypeMap.GetClrTypeFromField(field).Should().Be<string>();
	}

	[Theory]
	[InlineData(new byte[] { 0xFF, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, "00:00:00+00:00")]
	[InlineData(new byte[] { 0xFF, 0xE0, 0x00, 0x40, 0x42, 0x0F, 0x00, 0x00 }, "00:00:01+00:00")]
	[InlineData(new byte[] { 0xDF, 0xC4, 0x00, 0x00, 0xB0, 0xEB, 0x0E, 0x0A }, "12:00:00+02:00")]
	[InlineData(new byte[] { 0x4F, 0x27, 0x01, 0x00, 0xB0, 0xEB, 0x0E, 0x0A }, "12:00:00-05:00")]
	public void GetValue_ShouldDecodeAZoneQualifiedTime(byte[] payload, string expected)
	{
		var field = OpaqueField("time_tz", new FixedSizeBinaryType(8));

		ArrowTypeMap.GetValue(BuildFixedSizeBinary(payload), 0, field).Should().Be(expected);
	}

	[Theory]
	[InlineData(new byte[] { 0x07, 0xFE }, "0")]
	[InlineData(new byte[] { 0x07, 0xFF }, "1")]
	[InlineData(new byte[] { 0x05, 0xFD }, "101")]
	public void GetValue_ShouldDecodeABitString(byte[] payload, string expected)
	{
		var field = OpaqueField("bit", BinaryType.Default);

		ArrowTypeMap.GetValue(BuildBinary(payload), 0, field).Should().Be(expected);
	}

	[Theory]
	[InlineData(new byte[] { 0x80, 0x00, 0x01, 0x00 }, "0")]
	[InlineData(new byte[] { 0x80, 0x00, 0x01, 0x7B }, "123")]
	[InlineData(new byte[] { 0x7F, 0xFF, 0xFE, 0x84 }, "-123")]
	public void GetValue_ShouldDecodeAnArbitraryPrecisionInteger(byte[] payload, string expected)
	{
		var field = OpaqueField("bignum", BinaryType.Default);

		ArrowTypeMap.GetValue(BuildBinary(payload), 0, field).Should().Be(expected);
	}

	[Fact]
	public void GetValue_ShouldKeepRawBytes_ForAnUnknownOpaqueType()
	{
		// An unknown payload has no text form anyone can trust. Returning the bytes is the only
		// honest answer; inventing one would be a plausible wrong value.
		var payload = new byte[] { 1, 2, 3, 4 };
		var field = OpaqueField("something_new", new FixedSizeBinaryType(4));

		ArrowTypeMap.GetValue(BuildFixedSizeBinary(payload), 0, field).Should().BeEquivalentTo(payload);
		ArrowTypeMap.GetClrTypeFromField(field).Should().Be<byte[]>();
	}

	// ── List element typing ──────────────────────────────────────────────────────────

	[Fact]
	public void GetClrType_ShouldTypeAListOnItsElement()
	{
		// A writer builds its column schema from this type. Left as List<object?>, Parquet
		// settled on a string column and threw when the first int arrived.
		var type = new ListType(new Field("item", Int32Type.Default, true));

		ArrowTypeMap.GetClrType(type).Should().Be<List<int>>();
	}

	[Fact]
	public void GetClrType_ShouldTypeAFixedSizeListOnItsElement()
	{
		var type = new FixedSizeListType(new Field("item", DoubleType.Default, true), 3);

		ArrowTypeMap.GetClrType(type).Should().Be<List<double>>();
	}

	[Fact]
	public void GetClrType_ShouldTypeANestedListAllTheWayDown()
	{
		var type = new ListType(new Field("item", new ListType(new Field("inner", Int32Type.Default, true)), true));

		ArrowTypeMap.GetClrType(type).Should().Be<List<List<int>>>();
	}

	// ── Map ──────────────────────────────────────────────────────────────────────────

	[Fact]
	public void GetValue_ShouldReadAMapAsADictionary_NotAsAListOfEntries()
	{
		// MapArray derives from ListArray, so the general list case used to win and returned a
		// list of {key,value} structs — contradicting the Dictionary that GetClrType declares.
		var builder = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
		var keys = (StringArray.Builder)builder.KeyBuilder;
		var values = (Int32Array.Builder)builder.ValueBuilder;

		builder.Append();
		keys.Append("host");
		values.Append(7);

		var array = builder.Build();
		var value = ArrowTypeMap.GetValue(array, 0);

		value.Should().BeOfType<Dictionary<object, object?>>();
		((Dictionary<object, object?>)value!)["host"].Should().Be(7);
	}

	// ── Helpers ──────────────────────────────────────────────────────────────────────

	private static FixedSizeBinaryArray BuildFixedSizeBinary(byte[] payload)
	{
		var type = new FixedSizeBinaryType(payload.Length);
		var data = new ArrayData(type, 1, 0, 0,
			new[]
			{
				new ArrowBuffer.BitmapBuilder().Append(true).Build(),
				new ArrowBuffer.Builder<byte>().Append(payload).Build()
			});
		return new FixedSizeBinaryArray(data);
	}

	private static BinaryArray BuildBinary(byte[] payload)
		=> new BinaryArray.Builder().Append(payload.AsSpan()).Build();
}
