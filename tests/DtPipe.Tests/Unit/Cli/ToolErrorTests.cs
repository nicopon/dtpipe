using System;
using DtPipe.Cli.Mcp;
using Xunit;
using YamlDotNet.Core;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// What a failing tool tells a caller that cannot see a stack trace. Drawn from a real trace: a
/// model handed "No node deserializer was able to deserialize the node into type
/// DtPipe.Core.Models.JobDefinition, DtPipe.Core, Version=1.7.0.0, …" answered, in its own words,
/// that the structure was "slightly off" and guessed twice more before recovering.
/// </summary>
public class ToolErrorTests
{
    private static YamlException Yaml(string message, int line = 4, int column = 3) =>
        new(new Mark(0, line, column), new Mark(0, line, column + 1), "while parsing a job",
            new InvalidOperationException(message));

    [Fact]
    public void A_Yaml_Failure_Says_Where_In_The_Caller_Own_Input_It_Was()
    {
        var described = ToolError.Describe(Yaml("something went wrong"));

        Assert.Contains("line 4", described);
        Assert.Contains("column 3", described);
        Assert.Contains("something went wrong", described);
    }

    [Fact]
    public void An_Assembly_Qualified_Type_Is_Reduced_To_The_Type()
    {
        var described = ToolError.Describe(Yaml(
            "No node deserializer was able to deserialize the node into type "
            + "DtPipe.Core.Models.JobDefinition, DtPipe.Core, Version=1.7.0.0, Culture=neutral, PublicKeyToken=null"));

        Assert.Contains("into type JobDefinition", described);
        Assert.DoesNotContain("PublicKeyToken", described);
        Assert.DoesNotContain("Version=", described);
    }

    /// <summary>
    /// The deserializer names the type it wanted, which tells a caller nothing to act on. A later
    /// trace shows the cost: the model answered this error by inventing a 'job:' wrapper, which
    /// parsed as a branch of that name and validated, and the wrong guess cost the rest of the turn.
    /// </summary>
    [Fact]
    public void A_Job_Shape_Failure_Says_What_The_Top_Level_Is()
    {
        var described = ToolError.Describe(Yaml(
            "No node deserializer was able to deserialize the node into type JobDefinition"));

        Assert.Contains("map of branch aliases", described);
        Assert.Contains("'help'", described);
    }

    /// <summary>The hint belongs to that one failure; every other message stays as it is.</summary>
    [Fact]
    public void Another_Failure_Gets_No_Shape_Hint()
        => Assert.DoesNotContain("branch aliases", ToolError.Describe(Yaml("mapping values are not allowed here")));

    [Fact]
    public void A_Plain_Failure_Keeps_Its_Message()
    {
        Assert.Equal("the endpoint refused the connection",
            ToolError.Describe(new InvalidOperationException("the endpoint refused the connection")));
    }

    /// <summary>
    /// The line and column are only "where" once the caller can see the line. A recorded session
    /// submitted its job as one escaped JSON string, was told "line 18, column 41" three times,
    /// and escaped by replacing every expression in the job with a constant.
    /// </summary>
    [Fact]
    public void The_Failing_Line_Is_Quoted_With_A_Caret_Under_The_Column()
    {
        const string source = "categories:\n"
                            + "  input: \"generate:10\"\n"
                            + "  transformers:\n"
                            + "    - type: compute\n"
                            + "      mappings:\n"
                            + "        description: \"Description for \" + \"category\"\n";

        var described = ToolError.Describe(
            Yaml("While parsing a block mapping, did not find expected key.", line: 6, column: 41), source);

        Assert.Contains("6 |         description: \"Description for \" + \"category\"", described);
        Assert.Contains("^", described);
        Assert.Equal(40, described.Split('\n')[^1].IndexOf('^') - "  | ".Length);
    }

    /// <summary>Without the source there is nothing to quote, and the message stays as it was.</summary>
    [Fact]
    public void No_Source_Means_No_Excerpt()
        => Assert.DoesNotContain("|", ToolError.Describe(Yaml("did not find expected key.")));

    /// <summary>A mark past the end of what was sent quotes nothing rather than throwing.</summary>
    [Fact]
    public void A_Mark_Beyond_The_Source_Quotes_Nothing()
        => Assert.DoesNotContain("|", ToolError.Describe(Yaml("did not find expected key.", line: 99), "main:\n"));

    /// <summary>A long line is windowed around the column so an inline query cannot flood the reply.</summary>
    [Fact]
    public void A_Long_Line_Is_Windowed_Around_The_Column()
    {
        var source = "  query: \"" + new string('x', 400) + "\"";
        var described = ToolError.Describe(Yaml("did not find expected key.", line: 1, column: 300), source);

        Assert.Contains("…", described);
        Assert.All(described.Split('\n'), l => Assert.True(l.Length < 200, l));
    }

    /// <summary>An error message reaches a model and a log; a credential must reach neither.</summary>
    [Fact]
    public void A_Credential_In_The_Message_Is_Still_Blanked()
    {
        var described = ToolError.Describe(new InvalidOperationException(
            "login failed for Host=db;Username=app;Password=hunter2"));

        Assert.DoesNotContain("hunter2", described);
        Assert.Contains("login failed", described);
    }
}
