using System.Collections.Generic;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <see cref="AliasBindingApplier"/> substitutes --bind-input/--bind-output onto a job dictionary
/// before <see cref="PipelineValidator"/> sees it.
/// </summary>
public class AliasBindingApplierTests
{
    private static Dictionary<string, JobDefinition> Jobs(params (string Alias, string? Input, string? Output)[] branches)
    {
        var jobs = new Dictionary<string, JobDefinition>(System.StringComparer.OrdinalIgnoreCase);
        foreach (var (alias, input, output) in branches)
            jobs[alias] = new JobDefinition { Input = input, Output = output };
        return jobs;
    }

    [Fact]
    public void ABoundInput_ReplacesTheStreamLinkWithTheLocation()
    {
        var jobs = Jobs(("anonymiser", "arrow:-", "arrow:-"));

        var errors = AliasBindingApplier.Apply(jobs, "anonymiser=/run/42/in", null);

        Assert.Empty(errors);
        Assert.Equal("arrow:/run/42/in", jobs["anonymiser"].Input);
        Assert.Equal("arrow:-", jobs["anonymiser"].Output);
    }

    [Fact]
    public void ABoundOutput_ReplacesTheStreamLinkWithTheLocation()
    {
        var jobs = Jobs(("anonymiser", "arrow:-", "arrow:-"));

        var errors = AliasBindingApplier.Apply(jobs, null, "anonymiser=/run/42/out");

        Assert.Empty(errors);
        Assert.Equal("arrow:-", jobs["anonymiser"].Input);
        Assert.Equal("arrow:/run/42/out", jobs["anonymiser"].Output);
    }

    [Fact]
    public void ABranchCanBindBothEnds()
    {
        var jobs = Jobs(("anonymiser", "arrow:-", "arrow:-"));

        var errors = AliasBindingApplier.Apply(jobs, "anonymiser=/run/42/in", "anonymiser=/run/42/out");

        Assert.Empty(errors);
        Assert.Equal("arrow:/run/42/in", jobs["anonymiser"].Input);
        Assert.Equal("arrow:/run/42/out", jobs["anonymiser"].Output);
    }

    // ── Rule 1: only the link binds ──────────────────────────────────────

    [Theory]
    [InlineData("pg:Host=h;Database=d")]
    [InlineData("arrow:/data/x.arrow")]
    [InlineData(null)]
    public void ABranchWhoseInputIsNotTheStreamLink_IsRejected(string? actualInput)
    {
        var jobs = Jobs(("main", actualInput, "out.csv"));

        var errors = AliasBindingApplier.Apply(jobs, "main=/run/42/in", null);

        var error = Assert.Single(errors);
        Assert.Contains("main", error);
        Assert.Contains("arrow:-", error);
    }

    // ── Rule 2: a location, not an adapter ───────────────────────────────

    [Theory]
    [InlineData("/run/42/frag.arrow")]
    [InlineData("/run/42/frag.arrowfile")]
    public void ALocationEndingInTheArrowFileExtension_IsRejected(string location)
    {
        var jobs = Jobs(("main", "arrow:-", "out.csv"));

        var errors = AliasBindingApplier.Apply(jobs, $"main={location}", null);

        var error = Assert.Single(errors);
        Assert.Contains("main", error);
        Assert.Contains(location, error);
    }

    [Fact]
    public void APathOrANamedPipeLocation_IsAccepted()
    {
        var jobs = Jobs(("main", "arrow:-", "out.csv"), ("other", "arrow:-", "out2.csv"));

        var errors = AliasBindingApplier.Apply(jobs, "main=/run/42/in,other=pipe://run-42-other", null);

        Assert.Empty(errors);
        Assert.Equal("arrow:/run/42/in", jobs["main"].Input);
        Assert.Equal("arrow:pipe://run-42-other", jobs["other"].Input);
    }

    // ── Rule 3: unknown alias, or the same (branch, direction) pair twice ─

    [Fact]
    public void AnUnknownAlias_IsRejectedByName()
    {
        var jobs = Jobs(("main", "arrow:-", "out.csv"));

        var errors = AliasBindingApplier.Apply(jobs, "nosuchbranch=/run/42/in", null);

        var error = Assert.Single(errors);
        Assert.Contains("nosuchbranch", error);
    }

    [Fact]
    public void TheSameAliasTwiceInOneFlag_IsRejected()
    {
        var jobs = Jobs(("main", "arrow:-", "out.csv"));

        var errors = AliasBindingApplier.Apply(jobs, "main=/run/42/a,main=/run/42/b", null);

        var error = Assert.Single(errors);
        Assert.Contains("main", error);
        Assert.Contains("more than once", error);
    }

    [Fact]
    public void TheSameAlias_OnBothDirections_IsAccepted()
    {
        // The pair (branch, direction) is what must be unique, not the alias alone: a branch
        // legitimately binds both its input and its output.
        var jobs = Jobs(("anonymiser", "arrow:-", "arrow:-"));

        var errors = AliasBindingApplier.Apply(jobs, "anonymiser=/run/42/in", "anonymiser=/run/42/out");

        Assert.Empty(errors);
    }

    // ── Rule 5: substitution happens on the dictionary, so downstream sees only the bound job ──

    [Fact]
    public void AnInvalidEntry_LeavesOtherValidEntriesApplied()
    {
        var jobs = Jobs(("good", "arrow:-", "out.csv"), ("bad", "pg:Host=h", "out2.csv"));

        var errors = AliasBindingApplier.Apply(jobs, "good=/run/42/in,bad=/run/42/in2", null);

        Assert.Single(errors);
        Assert.Equal("arrow:/run/42/in", jobs["good"].Input);
        Assert.Equal("pg:Host=h", jobs["bad"].Input);
    }
}
