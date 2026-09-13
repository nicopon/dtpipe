using AwesomeAssertions;
using DtPipe.Core.Infrastructure.Arrow;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

/// <summary>
/// A reader has to know whether a column type has an Arrow form before it builds a schema around
/// it. Asking used to mean catching: the DuckDB reader threw on a STRUCT — which arrives as a
/// Dictionary — while building its schema, before a single row was read.
/// </summary>
public class ArrowTypeMapperTryTests
{
	[Theory]
	[InlineData(typeof(int))]
	[InlineData(typeof(string))]
	[InlineData(typeof(decimal))]
	[InlineData(typeof(byte[]))]
	[InlineData(typeof(Guid))]
	[InlineData(typeof(DateTime))]
	public void A_scalar_has_an_arrow_form(Type clrType)
		=> ArrowTypeMapper.TryGetLogicalType(clrType, out _).Should().BeTrue();

	[Theory]
	[InlineData(typeof(int[]))]
	[InlineData(typeof(List<string>))]
	[InlineData(typeof(int?[]))]
	public void A_collection_has_one_too(Type clrType)
		=> ArrowTypeMapper.TryGetLogicalType(clrType, out _).Should().BeTrue(
			"a list maps to an Arrow ListType and must not be diverted to text");

	[Theory]
	[InlineData(typeof(Dictionary<string, object>))]
	[InlineData(typeof(Dictionary<string, int>))]
	public void A_dictionary_has_none(Type clrType)
		=> ArrowTypeMapper.TryGetLogicalType(clrType, out _).Should().BeFalse(
			"this is how a DuckDB STRUCT and MAP arrive, and what the adapter renders as JSON");

	[Fact]
	public void Answering_no_is_not_the_same_as_throwing()
	{
		var act = () => ArrowTypeMapper.TryGetLogicalType(typeof(Dictionary<string, object>), out _);
		act.Should().NotThrow();
	}
}
