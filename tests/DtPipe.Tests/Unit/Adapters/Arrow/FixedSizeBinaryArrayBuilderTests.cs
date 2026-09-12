using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Reflection;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Arrow;

/// <summary>
/// The builder writes cells into one reused buffer rather than into a per-cell array, so the
/// cases that matter are the ones where a stale byte or a stale validity bit could survive:
/// a short input, a null, and a Clear() followed by fewer cells than the round before.
/// </summary>
public class FixedSizeBinaryArrayBuilderTests
{
	[Fact]
	public void Append_ShorterThanWidth_ZeroPadsOnTheRight()
	{
		var builder = new FixedSizeBinaryArrayBuilder(4);
		builder.Append(new byte[] { 0xAA, 0xBB });

		var array = (FixedSizeBinaryArray)builder.Build();

		array.GetBytes(0).ToArray().Should().Equal(0xAA, 0xBB, 0x00, 0x00);
	}

	[Fact]
	public void Append_LongerThanWidth_TruncatesToWidth()
	{
		var builder = new FixedSizeBinaryArrayBuilder(2);
		builder.Append(new byte[] { 0x01, 0x02, 0x03, 0x04 });

		var array = (FixedSizeBinaryArray)builder.Build();

		array.Length.Should().Be(1);
		array.GetBytes(0).ToArray().Should().Equal(0x01, 0x02);
	}

	[Fact]
	public void AppendNull_MarksInvalidAndZeroesItsSlot()
	{
		var builder = new FixedSizeBinaryArrayBuilder(4);
		builder.Append(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });
		builder.AppendNull();
		builder.Append(new byte[] { 0x11, 0x22, 0x33, 0x44 });

		var array = (FixedSizeBinaryArray)builder.Build();

		array.Length.Should().Be(3);
		array.NullCount.Should().Be(1);
		array.IsValid(0).Should().BeTrue();
		array.IsNull(1).Should().BeTrue();
		array.IsValid(2).Should().BeTrue();
		array.GetBytes(0).ToArray().Should().Equal(0xFF, 0xFF, 0xFF, 0xFF);
		array.GetBytes(2).ToArray().Should().Equal(0x11, 0x22, 0x33, 0x44);
	}

	/// <summary>
	/// Growth crosses both the initial capacity and a bitmap byte, so a validity bit landing in
	/// the wrong byte shows up here and nowhere in the small cases.
	/// </summary>
	[Fact]
	public void Append_PastTheInitialCapacity_KeepsEveryCellAndItsValidity()
	{
		const int count = 200;
		var builder = new FixedSizeBinaryArrayBuilder(2);
		for (int i = 0; i < count; i++)
		{
			if (i % 7 == 0) builder.AppendNull();
			else builder.Append(new[] { (byte)(i & 0xFF), (byte)0x5A });
		}

		var array = (FixedSizeBinaryArray)builder.Build();

		array.Length.Should().Be(count);
		array.NullCount.Should().Be(count / 7 + 1);
		for (int i = 0; i < count; i++)
		{
			if (i % 7 == 0)
			{
				array.IsNull(i).Should().BeTrue($"cell {i} was appended as null");
			}
			else
			{
				array.IsValid(i).Should().BeTrue($"cell {i} was appended with a value");
				array.GetBytes(i).ToArray().Should().Equal((byte)(i & 0xFF), (byte)0x5A);
			}
		}
	}

	[Fact]
	public void Clear_ThenFewerCells_LeavesNoValidityFromTheRoundBefore()
	{
		var builder = new FixedSizeBinaryArrayBuilder(2);
		for (int i = 0; i < 20; i++) builder.Append(new byte[] { 0x01, 0x02 });

		builder.Clear();
		builder.Length.Should().Be(0);

		builder.AppendNull();
		builder.Append(new byte[] { 0x09, 0x08 });

		var array = (FixedSizeBinaryArray)builder.Build();

		array.Length.Should().Be(2);
		array.NullCount.Should().Be(1);
		array.IsNull(0).Should().BeTrue();
		array.GetBytes(1).ToArray().Should().Equal(0x09, 0x08);
	}

	[Fact]
	public void Reserve_TakesACellCount_NotAByteCount()
	{
		var builder = new FixedSizeBinaryArrayBuilder(16);
		builder.Reserve(1_000);
		builder.Length.Should().Be(0);

		builder.Append(new byte[16]);
		builder.Length.Should().Be(1);
		((FixedSizeBinaryArray)builder.Build()).Length.Should().Be(1);
	}

	[Fact]
	public void Build_WithNothingAppended_IsAnEmptyArray()
	{
		var array = (FixedSizeBinaryArray)new FixedSizeBinaryArrayBuilder(16).Build();

		array.Length.Should().Be(0);
		array.NullCount.Should().Be(0);
	}

	[Fact]
	public void Constructor_RejectsANonPositiveWidth()
	{
		var act = () => new FixedSizeBinaryArrayBuilder(0);
		act.Should().Throw<ArgumentOutOfRangeException>();
	}
}
