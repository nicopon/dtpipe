using System;
using System.Collections.Generic;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Cursor;

public class CursorStateValidatorTests
{
    [Fact]
    public void Validate_NoState_NoErrors()
    {
        var dag = new JobDagDefinition
        {
            Branches = new List<BranchDefinition>
            {
                new BranchDefinition { Alias = "branch1" },
                new BranchDefinition { Alias = "branch2" }
            }
        };

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["branch1"] = new JobDefinition(),
            ["branch2"] = new JobDefinition()
        };

        var errors = CursorStateValidator.Validate(dag, jobs);
        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_DifferentStates_NoErrors()
    {
        var dag = new JobDagDefinition
        {
            Branches = new List<BranchDefinition>
            {
                new BranchDefinition { Alias = "branch1" },
                new BranchDefinition { Alias = "branch2" }
            }
        };

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["branch1"] = new JobDefinition { State = "state1.sync" },
            ["branch2"] = new JobDefinition { State = "state2.sync" }
        };

        var errors = CursorStateValidator.Validate(dag, jobs);
        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_SameState_ReturnsError()
    {
        var dag = new JobDagDefinition
        {
            Branches = new List<BranchDefinition>
            {
                new BranchDefinition { Alias = "branch1" },
                new BranchDefinition { Alias = "branch2" }
            }
        };

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["branch1"] = new JobDefinition { State = "shared.sync" },
            ["branch2"] = new JobDefinition { State = "shared.sync" }
        };

        var errors = CursorStateValidator.Validate(dag, jobs);
        Assert.Single(errors);
        Assert.Contains("shared.sync", errors[0]);
        Assert.Contains("branch1", errors[0]);
        Assert.Contains("branch2", errors[0]);
    }

    [Fact]
    public void Validate_SameStateDifferentCase_ReturnsError()
    {
        var dag = new JobDagDefinition
        {
            Branches = new List<BranchDefinition>
            {
                new BranchDefinition { Alias = "branch1" },
                new BranchDefinition { Alias = "branch2" }
            }
        };

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["branch1"] = new JobDefinition { State = "Shared.sync" },
            ["branch2"] = new JobDefinition { State = "shared.sync" }
        };

        var errors = CursorStateValidator.Validate(dag, jobs);
        Assert.Single(errors);
        Assert.Contains("shared.sync", errors[0], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_OneStateOneWithout_NoErrors()
    {
        var dag = new JobDagDefinition
        {
            Branches = new List<BranchDefinition>
            {
                new BranchDefinition { Alias = "branch1" },
                new BranchDefinition { Alias = "branch2" }
            }
        };

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["branch1"] = new JobDefinition { State = "state1.sync" },
            ["branch2"] = new JobDefinition()
        };

        var errors = CursorStateValidator.Validate(dag, jobs);
        Assert.Empty(errors);
    }
}
