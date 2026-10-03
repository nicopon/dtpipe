using DtPipe.Adapters.Common;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Common;

/// <summary>
/// The guard names the first keyword the server would run. Skipping a comment is only safe if the
/// keyword read afterwards is the one executed, so every refusal below is as much a part of the
/// contract as the queries that pass.
/// </summary>
public class SqlQueryGuardTests
{
    [Theory]
    [InlineData("SELECT 1")]
    [InlineData("  \n\tselect 1")]
    [InlineData("WITH t AS (SELECT 1) SELECT * FROM t")]
    [InlineData("-- the report\nSELECT 1")]
    [InlineData("-- one\r\n-- two\r\nSELECT 1")]
    [InlineData("/* the report */ SELECT 1")]
    [InlineData("/* a */ -- b\n /* c */\nWITH t AS (SELECT 1) SELECT * FROM t")]
    [InlineData("/* DELETE FROM t */ SELECT 1")]
    [InlineData("-- DELETE FROM t\nSELECT 1")]
    public void A_Read_Only_Statement_Passes_Whatever_Comments_Lead_It(string query)
        => SqlQueryGuard.RequireReadOnlyStatement(query);

    [Theory]
    [InlineData("DELETE FROM t")]
    [InlineData("/* x */ DELETE FROM t")]
    [InlineData("-- c\nDELETE FROM t")]
    [InlineData("-- c\r\nDROP TABLE t")]
    [InlineData("/* a */ /* b */ UPDATE t SET a = 1")]
    [InlineData("-- SELECT 1\nINSERT INTO t VALUES (1)")]
    public void A_Statement_That_Writes_Is_Refused_Behind_Any_Comment(string query)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryGuard.RequireReadOnlyStatement(query));

        Assert.Contains("blocked for safety", ex.Message);
    }

    [Theory]
    [InlineData("/*! DELETE FROM t */ SELECT 1", "MySQL")]
    [InlineData("/*!50000 DELETE FROM t */", "MySQL")]
    [InlineData("/* /* */ SELECT 1 */ DELETE FROM t", "another /*")]
    [InlineData("/* /* */ DELETE FROM t; /* */ */ SELECT 1", "another /*")]
    [InlineData("/* never closed SELECT 1", "never closed")]
    [InlineData("-- only a comment", "must start with a keyword")]
    [InlineData("(SELECT 1)", "must start with a keyword")]
    public void A_Comment_The_Engines_Read_Differently_Is_Refused_And_Says_Why(string query, string reason)
    {
        var ex = Assert.Throws<ArgumentException>(() => SqlQueryGuard.RequireReadOnlyStatement(query));

        Assert.Contains("Invalid query format", ex.Message);
        Assert.Contains(reason, ex.Message);
    }

    [Fact]
    public void A_Reader_Adds_Its_Own_Keywords_And_Nothing_Else()
    {
        SqlQueryGuard.RequireReadOnlyStatement("/* c */ VALUES (1)", "VALUES");
        SqlQueryGuard.RequireReadOnlyStatement("pragma table_info(t)", "PRAGMA", "DESCRIBE");

        Assert.Throws<InvalidOperationException>(() => SqlQueryGuard.RequireReadOnlyStatement("VALUES (1)"));
        Assert.Throws<InvalidOperationException>(() => SqlQueryGuard.RequireReadOnlyStatement("DELETE FROM t", "VALUES"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void An_Empty_Query_Is_Refused(string query)
        => Assert.Throws<ArgumentException>(() => SqlQueryGuard.RequireReadOnlyStatement(query));
}
