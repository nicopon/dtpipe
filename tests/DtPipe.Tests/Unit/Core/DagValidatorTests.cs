using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Validation;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class DagValidatorTests
{
    [Fact]
    public void ValidLinearPipeline_HasNoErrors()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "s1", Input = "gen:", Output = "csv:-" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidDag_HasNoErrors()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "src1", Input = "gen:1" },
                new BranchDefinition { Alias = "src2", Input = "gen:2" },
                new BranchDefinition
                {
                    Alias = "join",
                    ProcessorName = "sql",
                    StreamingAliases = new[] { "src1" },
                    RefAliases = new[] { "src2" },
                    Output = "csv:-"
                }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Empty(errors);
    }

    [Fact]
    public void Cycle_A_to_A_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition
                {
                    Alias = "loop",
                    ProcessorName = "sql",
                    StreamingAliases = new[] { "loop" }
                }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("Circular dependency detected"));
    }

    [Fact]
    public void Cycle_A_to_B_to_A_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "A", ProcessorName = "sql", StreamingAliases = new[] { "B" } },
                new BranchDefinition { Alias = "B", ProcessorName = "sql", StreamingAliases = new[] { "A" } }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("Circular dependency detected"));
    }

    [Fact]
    public void MissingFromAlias_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "XS", ProcessorName = "sql" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("missing its main source alias"));
    }

    [Fact]
    public void UnknownAlias_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "XS", ProcessorName = "sql", StreamingAliases = new[] { "ghost" } }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("references unknown streaming alias 'ghost'"));
    }

    [Fact]
    public void OutputInUpstreamBranch_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "src1", Input = "gen:", Output = "csv:leak.csv" },
                new BranchDefinition { Alias = "XS", ProcessorName = "sql", StreamingAliases = new[] { "src1" }, Output = "csv:-" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("cannot have its own '--output'"));
    }

    [Fact]
    public void MergeProcessor_ValidWith2Sources_HasNoErrors()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "a", Input = "gen:5" },
                new BranchDefinition { Alias = "b", Input = "gen:5" },
                new BranchDefinition
                {
                    Alias = "merged",
                    StreamingAliases = new[] { "a", "b" },
                    ProcessorName = "merge",
                    Arguments = new[] { "--from", "a,b", "--merge" },
                    Output = "csv:-"
                }
            }
        };

        var factories = new[] { new DtPipe.Processors.Merge.MergeTransformerFactory() };
        var errors = DagValidator.Validate(dag, factories);

        Assert.Empty(errors);
    }

    [Fact]
    public void MergeProcessor_With1Source_ReportsCapabilityError()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "a", Input = "gen:5" },
                new BranchDefinition
                {
                    Alias = "merged",
                    StreamingAliases = new[] { "a" },
                    ProcessorName = "merge",
                    Arguments = new[] { "--from", "a", "--merge" },
                    Output = "csv:-"
                }
            }
        };

        var factories = new[] { new DtPipe.Processors.Merge.MergeTransformerFactory() };
        var errors = DagValidator.Validate(dag, factories);

        Assert.Contains(errors, e => e.Contains("requires at least 2 streaming source"));
    }
}
