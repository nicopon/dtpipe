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
                    IsXStreamer = true,
                    MainAlias = "src1",
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
                    IsXStreamer = true,
                    MainAlias = "loop"
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
                new BranchDefinition { Alias = "A", IsXStreamer = true, MainAlias = "B" },
                new BranchDefinition { Alias = "B", IsXStreamer = true, MainAlias = "A" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("Circular dependency detected"));
    }

    [Fact]
    public void MissingMainAlias_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "XS", IsXStreamer = true }
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
                new BranchDefinition { Alias = "XS", IsXStreamer = true, MainAlias = "ghost" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("references unknown main alias 'ghost'"));
    }

    [Fact]
    public void OutputInUpstreamBranch_Detected()
    {
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "src1", Input = "gen:", Output = "csv:leak.csv" },
                new BranchDefinition { Alias = "XS", IsXStreamer = true, MainAlias = "src1", Output = "csv:-" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("cannot have its own '--output'"));
    }
}
