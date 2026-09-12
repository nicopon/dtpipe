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
        => Validate(new IStreamTransformerFactory[] { new FakeProcessor("sql", "--sql", false), new FakeProcessor("merge", "--merge", true) }, branches);

    private static List<string> Validate(IStreamTransformerFactory[] processors, params BranchDefinition[] branches)
    {
        var dag = new JobDagDefinition { Branches = branches };
        var jobs = branches.ToDictionary(
            b => b.Alias,
            b => new JobDefinition { Input = b.Input, Output = b.Output },
            StringComparer.OrdinalIgnoreCase);

        return PipelineValidator.Validate(dag, jobs, processors);
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
        Assert.Contains("--sql <value>", error);
        Assert.Contains("--merge", error);
    }

    /// <summary>
    /// The triggers come off the catalogue, not from a name written into the message: a registry
    /// carrying a different processor produces a different list, so adding, renaming or retiring
    /// one cannot leave the message behind.
    /// </summary>
    [Fact]
    public void The_Ref_Hint_Follows_The_Catalogue_Rather_Than_A_Fixed_Name()
    {
        var errors = Validate(
            new IStreamTransformerFactory[] { new FakeProcessor("pivot", "--unknown-processor", false) },
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition { Alias = "B", Input = "b.csv" },
            new BranchDefinition { Alias = "consumer", StreamingAliases = new[] { "A" }, RefAliases = new[] { "B" }, Output = "out.csv" });

        var error = Assert.Single(errors);
        Assert.Contains("--unknown-processor <value>", error);
        Assert.DoesNotContain("--sql", error);
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

    // ── A processor branch never reads an input of its own ───────────────

    /// <summary>
    /// The orchestrator injects no reader into a processor branch, and the input is not merely
    /// ignored — it is never opened. Measured on the binary: a processor branch pointed at a path
    /// that does not exist runs to completion and exits 0, while the same path without a processor
    /// fails on FileNotFoundException. So the source was named, never read, and nothing said so.
    ///
    /// The rule reads HasStreamTransformer rather than any flag, so every processor is covered by
    /// the same sentence and a new one needs no edit here.
    /// </summary>
    [Theory]
    [InlineData("sql")]
    [InlineData("merge")]
    [InlineData("a-processor-that-does-not-exist-yet")]
    public void A_Processor_Branch_With_An_Input_Is_Rejected(string processor)
    {
        var errors = Validate(
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition
            {
                Alias = "consumer",
                Input = "b.csv",
                StreamingAliases = new[] { "A" },
                ProcessorName = processor,
                Output = "out.csv"
            });

        var error = Assert.Single(errors);
        Assert.Contains("consumer", error);
        Assert.Contains(processor, error);
        Assert.Contains("b.csv", error);
        Assert.Contains("--from", error);
    }

    /// <summary>A connection string reaches the message past the repository's single sanitiser.</summary>
    [Fact]
    public void The_Refusal_Does_Not_Print_A_Password()
    {
        var errors = Validate(
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition
            {
                Alias = "consumer",
                Input = "pg:Host=h;Database=d;Username=u;Password=hunter2",
                StreamingAliases = new[] { "A" },
                ProcessorName = "sql",
                Output = "out.csv"
            });

        var error = Assert.Single(errors);
        Assert.DoesNotContain("hunter2", error);
    }

    [Fact]
    public void A_Processor_Branch_Without_An_Input_Is_Accepted()
    {
        var errors = Validate(
            new BranchDefinition { Alias = "A", Input = "a.csv" },
            new BranchDefinition
            {
                Alias = "consumer",
                StreamingAliases = new[] { "A" },
                ProcessorName = "sql",
                Output = "out.csv"
            });

        Assert.Empty(errors);
    }

    /// <summary>A branch with an input and no processor is the ordinary shape — untouched.</summary>
    [Fact]
    public void A_Branch_With_An_Input_And_No_Processor_Is_Accepted()
    {
        Assert.Empty(Validate(new BranchDefinition { Alias = "main", Input = "a.csv", Output = "out.csv" }));
    }

    private sealed class FakeProcessor : IStreamTransformerFactory
    {
        private readonly (string Flag, bool IsBoolean) _trigger;
        public FakeProcessor(string name, string flag, bool isBoolean)
        {
            ComponentName = name;
            _trigger = (flag, isBoolean);
        }

        public string ComponentName { get; }
        public string Category => "Stream Processors";
        public bool RequiresArrowChannels => true;
        public int MinStreams => 1;
        public int MaxStreams => -1;
        public int MinLookups => 0;
        public int MaxLookups => -1;
        public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => new[] { _trigger };

        public bool IsApplicable(string[] branchArgs) => false;
        public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException();
        public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException();
    }
}
