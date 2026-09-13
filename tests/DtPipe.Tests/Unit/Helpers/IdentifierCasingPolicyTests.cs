using AwesomeAssertions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Dialects;
using Xunit;

namespace DtPipe.Tests.Unit.Helpers;

/// <summary>
/// One policy for every engine: an identifier is handed over the way that engine reads it
/// unquoted, so the name it stores is the name any other tool can type. Each dialect states its
/// own folding — that is where they differ, and the only place they may.
///
/// A dialect that claims a folding its engine does not perform is the failure this guards: DuckDB
/// said it lowercased unquoted identifiers, which it does not, and quoted every mixed-case name
/// for a reason that did not exist.
/// </summary>
public class IdentifierCasingPolicyTests
{
	/// <summary>Every dialect, with the casing its engine applies to an unquoted identifier.</summary>
	private static readonly (string Because, ISqlDialect Dialect, string Folded)[] Engines =
	[
		("Oracle folds up",       new OracleDialect(),     "STOCK_MOVES"),
		("PostgreSQL folds down", new PostgreSqlDialect(), "stock_moves"),
		("MySQL does not fold",   new MySqlDialect(),      "Stock_Moves"),
		("SQL Server does not",   new SqlServerDialect(),  "Stock_Moves"),
		("SQLite does not",       new SqliteDialect(),     "Stock_Moves"),
		("DuckDB does not",       new DuckDbDialect(),     "Stock_Moves"),
	];

	public static TheoryData<string, ISqlDialect, string> Foldings
	{
		get
		{
			var data = new TheoryData<string, ISqlDialect, string>();
			foreach (var (because, dialect, folded) in Engines) data.Add(because, dialect, folded);
			return data;
		}
	}

	public static TheoryData<string, ISqlDialect> Dialects
	{
		get
		{
			var data = new TheoryData<string, ISqlDialect>();
			foreach (var (because, dialect, _) in Engines) data.Add(because, dialect);
			return data;
		}
	}

	[Theory]
	[MemberData(nameof(Foldings))]
	public void Each_dialect_states_its_own_folding(string because, ISqlDialect dialect, string expected)
		=> dialect.Normalize("Stock_Moves").Should().Be(expected, because);

	[Theory]
	[MemberData(nameof(Dialects))]
	public void A_normalized_identifier_no_longer_needs_quoting(string because, ISqlDialect dialect)
		=> dialect.NeedsQuoting(dialect.Normalize("Stock_Moves")).Should().BeFalse(
			$"{because}, so nothing about its case remains to preserve");

	[Theory]
	[MemberData(nameof(Dialects))]
	public void An_ordinary_lower_case_name_is_never_quoted_for_its_case(string because, ISqlDialect dialect)
		=> dialect.NeedsQuoting(dialect.Normalize("stock_moves")).Should().BeFalse(
			$"{because}, and the commonest name there is must stay reachable from any other tool");

	[Theory]
	[MemberData(nameof(Dialects))]
	public void A_reserved_word_is_still_quoted(string because, ISqlDialect dialect)
		=> dialect.NeedsQuoting(dialect.Normalize("select")).Should().BeTrue(
			$"{because}, but folding says nothing about a name the parser would claim");

	[Theory]
	[MemberData(nameof(Dialects))]
	public void A_name_with_a_special_character_is_still_quoted(string because, ISqlDialect dialect)
		=> dialect.NeedsQuoting(dialect.Normalize("stock moves")).Should().BeTrue($"{because}");
}
