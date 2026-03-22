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
                    SqlQuery = "SELECT * FROM src1 JOIN src2 ON src1.id = src2.id",
                    FromAlias = "src1",
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
                    SqlQuery = "SELECT 1",
                    FromAlias = "loop"
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
                new BranchDefinition { Alias = "A", SqlQuery = "SELECT 1", FromAlias = "B" },
                new BranchDefinition { Alias = "B", SqlQuery = "SELECT 1", FromAlias = "A" }
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
                new BranchDefinition { Alias = "XS", SqlQuery = "SELECT 1" }
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
                new BranchDefinition { Alias = "XS", SqlQuery = "SELECT 1", FromAlias = "ghost" }
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
                new BranchDefinition { Alias = "XS", SqlQuery = "SELECT 1", FromAlias = "src1", Output = "csv:-" }
            }
        };

        var errors = DagValidator.Validate(dag);

        Assert.Contains(errors, e => e.Contains("cannot have its own '--output'"));
    }
}
