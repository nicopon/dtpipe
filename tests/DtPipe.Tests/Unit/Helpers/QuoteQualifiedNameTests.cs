using AwesomeAssertions;
using DtPipe.Core.Dialects;
using DtPipe.Core.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Helpers;

/// <summary>
/// <c>--table</c> on the source side builds the query, and it used to quote the whole value as one
/// identifier: <c>eshop.customers</c> became a relation literally named "eshop.customers", and
/// MySQL got double quotes it only accepts under ANSI_QUOTES — so even an unqualified name failed
/// there. Every writer already split on the dot; this is the reader half.
/// </summary>
public class QuoteQualifiedNameTests
{
	[Fact]
	public void A_qualified_name_is_quoted_one_segment_at_a_time()
		=> SqlIdentifierHelper.QuoteQualifiedName(new PostgreSqlDialect(), "eshop.customers")
			.Should().Be("\"eshop\".\"customers\"");

	[Fact]
	public void An_unqualified_name_keeps_a_single_pair()
		=> SqlIdentifierHelper.QuoteQualifiedName(new PostgreSqlDialect(), "customers")
			.Should().Be("\"customers\"");

	[Fact]
	public void Three_parts_are_each_quoted()
		=> SqlIdentifierHelper.QuoteQualifiedName(new SqlServerDialect(), "shop.dbo.customers")
			.Should().Be("[shop].[dbo].[customers]");

	[Fact]
	public void Each_dialect_uses_its_own_characters()
	{
		SqlIdentifierHelper.QuoteQualifiedName(new MySqlDialect(), "shop.customers")
			.Should().Be("`shop`.`customers`", "MySQL accepts double quotes only under ANSI_QUOTES");
		SqlIdentifierHelper.QuoteQualifiedName(new SqlServerDialect(), "dbo.customers")
			.Should().Be("[dbo].[customers]");
		SqlIdentifierHelper.QuoteQualifiedName(new OracleDialect(), "hr.employees")
			.Should().Be("\"hr\".\"employees\"");
	}

	[Fact]
	public void A_value_that_already_carries_quotes_is_left_alone()
	{
		SqlIdentifierHelper.QuoteQualifiedName(new PostgreSqlDialect(), "\"an.awkward.name\"")
			.Should().Be("\"an.awkward.name\"", "the caller has said where the name starts and ends");
		SqlIdentifierHelper.QuoteQualifiedName(new SqlServerDialect(), "[dbo].[customers]")
			.Should().Be("[dbo].[customers]");
	}

	[Fact]
	public void No_dialect_falls_back_to_the_standard_form()
		=> SqlIdentifierHelper.QuoteQualifiedName(null, "eshop.customers")
			.Should().Be("\"eshop\".\"customers\"");

	[Fact]
	public void An_empty_value_is_returned_unchanged()
		=> SqlIdentifierHelper.QuoteQualifiedName(new PostgreSqlDialect(), "").Should().Be("");
}
