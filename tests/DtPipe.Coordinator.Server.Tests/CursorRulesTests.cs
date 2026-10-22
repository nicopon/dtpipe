using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>What a cursor may be, and where a brick may name one: a sink's column, never a source's, never the file that keeps it.</summary>
public sealed class CursorRulesTests
{
    [Theory]
    [InlineData("integer", "42", true)]
    [InlineData("integer", "-7", true)]
    [InlineData("integer", "4 2", false)]
    [InlineData("integer", "1; DROP TABLE t", false)]
    [InlineData("datetime", "2026-10-22", true)]
    [InlineData("datetime", "2026-10-22T10:30:00.123Z", true)]
    [InlineData("datetime", "2026-10-22T10:30:00+02:00", true)]
    [InlineData("datetime", "2026-10-22T10:30:00' OR '1'='1", false)]
    [InlineData("string", "ORD-0042/a@b", true)]
    [InlineData("string", "it's", false)]
    [InlineData("string", "a\\b", false)]
    [InlineData("string", "a;b", false)]
    [InlineData("blob", "x", false)]
    public void A_Value_Is_Accepted_Only_As_Its_Type_And_Cannot_End_The_String_It_Is_Written_In(string type, string value, bool accepted) =>
        Assert.Equal(accepted, CursorValues.Refusal(type, value) is null);

    [Fact]
    public void A_Value_Longer_Than_The_Bound_Or_Empty_Is_Refused()
    {
        Assert.NotNull(CursorValues.Refusal("string", new string('a', CursorValues.MaxLength + 1)));
        Assert.NotNull(CursorValues.Refusal("string", ""));
    }

    private static string? Declared(string json) => BrickRules.DeclaredRefusal("b", json);

    [Fact]
    public void A_Sink_May_Set_A_Cursor_Column_And_A_Source_May_Not()
    {
        Assert.Null(Declared("""{"output":"sqlite:b","cursor":"id"}"""));
        Assert.Contains("sets a cursor", Declared("""{"input":"sqlite:a","cursor":"id"}"""));
    }

    [Fact]
    public void The_File_That_Keeps_A_Cursor_Is_The_Nodes_And_Never_A_Bricks()
    {
        Assert.Contains("state", Declared("""{"output":"sqlite:b","cursor":"id","state":"/tmp/c.json"}"""));
        Assert.Contains("state", Declared("""{"input":"sqlite:a","state":"/tmp/c.json"}"""));
    }

    [Fact]
    public void A_Source_Token_Names_No_File()
    {
        Assert.Null(Declared("""{"input":"sqlite:a","provider-options":{"sqlite":{"query":"SELECT * FROM t WHERE id > ${{cursor://|0}}"}}}"""));
        var refused = Declared("""{"input":"sqlite:a","provider-options":{"sqlite":{"query":"SELECT * FROM t WHERE id > ${{cursor://orders.json|0}}"}}}""");
        Assert.Contains("orders.json", refused);
        Assert.Contains("${{cursor://|default}}", refused);
    }

    [Fact]
    public void A_Composed_Sink_Keeps_Its_Cursor_Column_And_The_Version_Follows_It()
    {
        var one = BrickRules.VersionOf(BrickKind.Sink, """{"output":"sqlite:b","cursor":"id"}""");
        var other = BrickRules.VersionOf(BrickKind.Sink, """{"output":"sqlite:b","cursor":"updated_at"}""");
        Assert.NotEqual(one, other);

        var composed = FragmentJob.Compose(
            [new("snk", FragmentUnitKind.Sink, "snk", "src"), new("src", FragmentUnitKind.Source, "src")],
            (id, kind) => (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(id == "snk" ? """{"output":"sqlite:b","cursor":"id"}""" : """{"input":"sqlite:a"}"""))).Yaml;
        Assert.Contains("cursor: id", composed);
    }
}
