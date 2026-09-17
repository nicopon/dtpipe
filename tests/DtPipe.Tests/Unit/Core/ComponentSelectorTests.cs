using DtPipe.Core.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// ComponentSelector is the single authority on the "{component}[+{variant}]:" grammar. It exists
/// because this logic used to be reimplemented at every routing site (pipeline, inspect, MCP
/// analyze, job export, DAG rendering, provider configuration) and the copies drifted apart.
/// </summary>
public class ComponentSelectorTests
{
    [Theory]
    [InlineData("duck:warehouse.duckdb", "duck", "warehouse.duckdb")]
    [InlineData("csv:data.csv", "csv", "data.csv")]
    [InlineData("pg:Host=localhost;Database=db;", "pg", "Host=localhost;Database=db;")]
    [InlineData("duck: spaced.duckdb ", "duck", "spaced.duckdb")]
    public void Plain_Selector_Is_Stripped(string raw, string component, string expected)
    {
        var selection = ComponentSelector.Select(raw, component);

        Assert.True(selection.Matched);
        Assert.Equal(expected, selection.Cleaned);
        Assert.Null(selection.Variant);
    }

    [Theory]
    [InlineData("duck+mysql:Host=localhost;Database=db;", "duck", "mysql", "Host=localhost;Database=db;")]
    [InlineData("DUCK+MySQL:Host=x;", "duck", "MySQL", "Host=x;")]
    public void Variant_Selector_Is_Split(string raw, string component, string expectedVariant, string expectedCleaned)
    {
        var selection = ComponentSelector.Select(raw, component);

        Assert.True(selection.Matched);
        Assert.Equal(expectedVariant, selection.Variant);
        Assert.Equal(expectedCleaned, selection.Cleaned);
    }

    /// <summary>
    /// The rule that produced the bugs this class was extracted to fix: "s3://bucket/key.parquet"
    /// starts with "s3:" but the "//" marks a URI scheme, not a selector. Stripping it handed the
    /// provider "//bucket/key.parquet". Only the pipeline path had learned this; inspect and the
    /// MCP tools had not. Expressing it in the grammar makes every site inherit it.
    /// </summary>
    [Theory]
    [InlineData("s3://bucket/key.parquet", "s3")]
    [InlineData("s3a://bucket/key.parquet", "s3a")]
    [InlineData("azure://container/blob.csv", "azure")]
    [InlineData("https://example.com/feed.jsonl", "https")]
    public void Remote_Uri_Is_Never_A_Selector(string raw, string component)
    {
        var selection = ComponentSelector.Select(raw, component);

        Assert.False(selection.Matched);
        Assert.Equal(raw, selection.Cleaned);
        Assert.Null(selection.Variant);
    }

    [Theory]
    [InlineData("csv", "csv")]
    [InlineData("CSV", "csv")]
    public void Bare_Component_Name_Selects_Stdio(string raw, string component)
    {
        var selection = ComponentSelector.Select(raw, component);

        Assert.True(selection.Matched);
        Assert.Equal("-", selection.Cleaned);
        Assert.Null(selection.Variant);
    }

    [Theory]
    [InlineData("parquet:data.parquet", "csv")]
    [InlineData("data.csv", "csv")]
    [InlineData("ducky:x", "duck")]
    [InlineData("duck+:whatever", "duck")]   // empty variant: malformed, so it is not a selector
    [InlineData("", "duck")]
    public void Non_Matching_Input_Leaves_The_String_Untouched(string raw, string component)
    {
        var selection = ComponentSelector.Select(raw, component);

        Assert.False(selection.Matched);
        Assert.Null(selection.Variant);
    }

    /// <summary>A Windows drive letter must not be mistaken for a one-letter component selector.</summary>
    [Fact]
    public void Windows_Drive_Letter_Is_Not_Claimed_By_Another_Component()
    {
        Assert.False(ComponentSelector.Select(@"C:\data\file.csv", "csv").Matched);
        Assert.False(ComponentSelector.Select(@"C:\data\file.csv", "duck").Matched);
    }

    [Fact]
    public void Matches_Agrees_With_Select()
    {
        Assert.True(ComponentSelector.Matches("duck+mysql:Host=x;", "duck"));
        Assert.True(ComponentSelector.Matches("duck:x.duckdb", "duck"));
        Assert.False(ComponentSelector.Matches("s3://bucket/k.parquet", "s3"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SkipSelector — the same grammar, with no name to match against.
    // ─────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("sqlserver:Password=p", "Password=p")]
    [InlineData("duck+mysql:Host=x;", "Host=x;")]
    [InlineData("azure:DefaultEndpointsProtocol=https", "DefaultEndpointsProtocol=https")]
    public void SkipSelector_Steps_Over_A_Selector(string raw, string expected)
        => Assert.Equal(expected, ComponentSelector.SkipSelector(raw));

    /// <summary>
    /// The remote-URI rule holds for the name-agnostic form too. A copy of this grammar that
    /// dropped the "(?!//)" would turn "s3://bucket/key" into "//bucket/key" — the failure that
    /// made ComponentSelector the single authority in the first place.
    /// </summary>
    [Theory]
    [InlineData("s3://bucket/key.parquet")]
    [InlineData("postgresql://postgres:pw@localhost:5432/mydb")]
    [InlineData("Host=localhost;Database=mydb")]
    [InlineData(@"C:\data\file.csv")]
    public void SkipSelector_Leaves_Everything_Else_Intact(string raw)
        => Assert.Equal(raw, ComponentSelector.SkipSelector(raw));
}
