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

    [Fact]
    public void A_Plain_Failure_Keeps_Its_Message()
    {
        Assert.Equal("the endpoint refused the connection",
            ToolError.Describe(new InvalidOperationException("the endpoint refused the connection")));
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
