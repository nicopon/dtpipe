using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <see cref="PipelineValidator"/> is the gate both the CLI (<c>JobService</c>) and the MCP
/// execute path run a DAG through before any branch starts.
/// </summary>
public class PipelineValidatorTests
{
    private static List<string> Validate(params BranchDefinition[] branches)
    {
        var dag = new JobDagDefinition { Branches = branches };
        var jobs = branches.ToDictionary(
            b => b.Alias,
            b => new JobDefinition { Input = b.Input, Output = b.Output },
            StringComparer.OrdinalIgnoreCase);

        return PipelineValidator.Validate(dag, jobs, Array.Empty<IStreamTransformerFactory>());
    }

    /// <summary>
    /// A ref has no consumer but a processor. Without one the referenced branch was read in full
    /// and thrown away, and the run reported success over a result missing its columns — the shape
    /// '--from a --ref b --query "&lt;join&gt;"' produced, where --sql was meant.
    /// </summary>
    [Fact]
    public void RefWithoutAProcessor_IsRejected()
    {
        var errors = Validate(
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition { Alias = "B", Input = "b.csv" },
            new BranchDefinition
            {
                Alias = "consumer",
                StreamingAliases = new[] { "A" },
                RefAliases = new[] { "B" },
                Output = "out.csv"
            });

        var error = Assert.Single(errors);
        Assert.Contains("consumer", error);
        Assert.Contains("B", error);
        Assert.Contains("--sql", error);
    }

    [Fact]
    public void RefWithAProcessor_IsAccepted()
    {
        var errors = Validate(
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition { Alias = "B", Input = "b.csv" },
            new BranchDefinition
            {
                Alias = "join",
                ProcessorName = "sql",
                StreamingAliases = new[] { "A" },
                RefAliases = new[] { "B" },
                Output = "out.csv"
            });

        Assert.Empty(errors);
    }

    /// <summary>A fan-out consumer carries a 'from' and no processor, and is legitimate.</summary>
    [Fact]
    public void FromWithoutARefOrAProcessor_IsAccepted()
    {
        var errors = Validate(
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition { Alias = "c1", StreamingAliases = new[] { "A" }, Output = "one.csv" },
            new BranchDefinition { Alias = "c2", StreamingAliases = new[] { "A" }, Output = "two.csv" });

        Assert.Empty(errors);
    }
}
