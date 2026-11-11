using System.Collections.Generic;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <see cref="EdgesOnlyRule"/>: under <c>--edges-only</c> the job's text names its <c>arrow:-</c> edges and nothing else,
/// and a binding gives them named pipes alone.
/// </summary>
public class EdgesOnlyRuleTests
{
    private static Dictionary<string, JobDefinition> Jobs(params (string Alias, JobDefinition Job)[] branches)
    {
        var jobs = new Dictionary<string, JobDefinition>(System.StringComparer.OrdinalIgnoreCase);
        foreach (var (alias, job) in branches) jobs[alias] = job;
        return jobs;
    }

    [Fact]
    public void Edges_And_Branches_That_Read_A_Channel_Pass()
    {
        var jobs = Jobs(
            ("in", new JobDefinition { Input = "arrow:-" }),
            ("join", new JobDefinition { From = "in" }),
            ("out", new JobDefinition { From = "join", Output = "arrow:-" }));

        Assert.Empty(EdgesOnlyRule.TextRefusals(jobs));
    }

    [Fact]
    public void A_Reader_Or_A_Writer_That_Is_No_Edge_Is_Refused_Naming_Its_Branch_And_Hiding_A_Secret()
    {
        var jobs = Jobs(
            ("leak_in", new JobDefinition { From = "s", Output = "csv:tmp.csv" }),
            ("leak_out", new JobDefinition { Input = "pg:Host=db;Password=hunter2" }),
            ("named", new JobDefinition { Input = "arrow:pipe://other" }));

        var refusals = EdgesOnlyRule.TextRefusals(jobs);

        Assert.Equal(3, refusals.Count);
        Assert.Contains(refusals, r => r.StartsWith("'leak_in' writes 'csv:tmp.csv'"));
        Assert.Contains(refusals, r => r.StartsWith("'leak_out' reads") && !r.Contains("hunter2"));
        Assert.Contains(refusals, r => r.StartsWith("'named' reads 'arrow:pipe://other'"));
    }

    [Theory]
    [InlineData("checkpoint")]
    [InlineData("from-checkpoint")]
    [InlineData("from-contract")]
    [InlineData("metrics-path")]
    [InlineData("cursor")]
    [InlineData("state")]
    public void A_Place_On_Disk_Is_Refused(string key)
    {
        var job = key switch
        {
            "checkpoint" => new JobDefinition { Checkpoint = "x" },
            "from-checkpoint" => new JobDefinition { FromCheckpoint = "x" },
            "from-contract" => new JobDefinition { FromContract = "x" },
            "metrics-path" => new JobDefinition { MetricsPath = "x" },
            "cursor" => new JobDefinition { Cursor = "x" },
            _ => new JobDefinition { State = "x" },
        };

        Assert.StartsWith($"'b' sets {key} 'x'", Assert.Single(EdgesOnlyRule.TextRefusals(Jobs(("b", job with { From = "a" })))));
    }

    [Fact]
    public void Once_Bound_An_Edge_Is_The_Process_Stdio_Or_A_Named_Pipe()
    {
        var jobs = Jobs(
            ("a", new JobDefinition { Input = "arrow:-", Output = "arrow:pipe://abc" }),
            ("b", new JobDefinition { Input = "arrow:/var/data/in" }));

        Assert.StartsWith("'b' reads 'arrow:/var/data/in' once bound", Assert.Single(EdgesOnlyRule.BoundRefusals(jobs)));
    }

    [Fact]
    public void A_File_Of_The_Working_Directory_May_Be_Bound_Or_Hold_A_Contract_And_No_Other()
    {
        var jobs = Jobs(
            ("src", new JobDefinition { Input = "arrow:src.arrows" }),
            ("dst", new JobDefinition { From = "src", Output = "arrow:dst.sink", ContractSave = "dst.contract.json" }));
        var elsewhere = Jobs(("dst", new JobDefinition { From = "src", ContractSave = "/tmp/dst.contract.json" }));

        Assert.Empty(EdgesOnlyRule.BoundRefusals(jobs));
        Assert.Empty(EdgesOnlyRule.TextRefusals(Jobs(("dst", jobs["dst"] with { Output = "arrow:-" }))));
        Assert.StartsWith("'dst' sets contract-save '/tmp/dst.contract.json'", Assert.Single(EdgesOnlyRule.TextRefusals(elsewhere)));
        Assert.Single(EdgesOnlyRule.BoundRefusals(Jobs(("x", new JobDefinition { Input = "arrow:../x" }))));
    }
}
