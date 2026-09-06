using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Pipeline;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The plan/DAG panel as UI-agnostic lines — one row per branch plus a
/// status badge from <see cref="PlanProgress"/>. Pure: no toolkit, asserted directly.
/// </summary>
public class PlanPanelTests
{
    private static DagTopology Topo(params BranchTopology[] branches) => new(branches);

    private static BranchTopology Branch(
        string alias, string? input = null, string? output = null, string? processor = null,
        string[]? from = null, string[]? @ref = null, string[]? transformers = null)
        => new(alias, input, output, processor,
            from ?? System.Array.Empty<string>(), @ref ?? System.Array.Empty<string>(),
            transformers ?? System.Array.Empty<string>());

    [Fact]
    public void No_Plan_Yet_Is_Just_The_Badge()
    {
        var lines = PlanPanelContent.Lines(PlanState.None, null, null);
        Assert.Equal(new[] { "○ no plan yet" }, lines);
    }

    [Fact]
    public void A_Drafted_Plan_Without_A_Parsed_Topology_Says_So()
    {
        var lines = PlanPanelContent.Lines(PlanState.Drafted, null, null);
        Assert.Equal(new[] { "(plan YAML not parsed yet)", "◐ drafted — not validated" }, lines);
    }

    /// <summary>
    /// A branch is every stage between its endpoints, in order. A row transformer is a stage inside
    /// the branch and appears nowhere else on the surface, so a plan whose whole substance is a
    /// fake or a compute would otherwise read as a bare copy from source to sink.
    /// </summary>
    [Fact]
    public void A_Branch_Renders_Every_Stage_Between_Its_Endpoints()
    {
        var lines = PlanPanelContent.Lines(PlanState.Validated, null,
            Topo(Branch("main", input: "generate:500", output: "csv:toto.csv",
                transformers: new[] { "fake", "compute" })));

        Assert.Equal("main: generate:500 → fake → compute → csv:toto.csv", lines[0]);
    }

    [Fact]
    public void A_Stream_Processor_Comes_After_The_Row_Transformers()
    {
        var lines = PlanPanelContent.Lines(PlanState.Validated, null,
            Topo(Branch("joined", from: new[] { "a", "b" }, output: "pg:out",
                processor: "sql", transformers: new[] { "mask" })));

        Assert.Equal("joined: a,b → mask → [sql] → pg:out", lines[0]);
    }

    [Fact]
    public void A_Linear_Branch_Renders_Source_Arrow_Output()
    {
        var lines = PlanPanelContent.Lines(PlanState.Validated, null,
            Topo(Branch("main", input: "csv:in.csv", output: "csv:out.csv")));

        Assert.Equal("main: csv:in.csv → csv:out.csv", lines[0]);
        Assert.Equal("✓ validated", lines[1]);
    }

    [Fact]
    public void A_Processor_Branch_Renders_From_Aliases_The_Processor_And_The_Ref_Suffix()
    {
        var lines = PlanPanelContent.Lines(PlanState.DryRunOk, null,
            Topo(
                Branch("p", input: "parquet:p.parquet"),
                Branch("joined", output: "pg:out", processor: "sql", from: new[] { "p" }, @ref: new[] { "c" })));

        Assert.Equal("p: parquet:p.parquet → (none)", lines[0]);
        Assert.Equal("joined: p → [sql] → pg:out  (ref: c)", lines[1]);
        Assert.Equal("✓ dry-run ok — nothing written", lines[2]);
    }

    [Fact]
    public void A_Channel_Only_Branch_Shows_A_Placeholder_Source()
    {
        var lines = PlanPanelContent.Lines(PlanState.Validated, null,
            Topo(Branch("sink", output: "csv:out.csv")));

        Assert.Equal("sink: (channel) → csv:out.csv", lines[0]);
    }

    [Fact]
    public void Connection_Strings_Are_Sanitised()
    {
        var lines = PlanPanelContent.Lines(PlanState.Validated, null,
            Topo(Branch("main", input: "pg:Host=db;Username=admin;Password=s3cr3t", output: "csv:out.csv")));

        Assert.DoesNotContain("s3cr3t", lines[0]);
    }

    [Fact]
    public void An_Invalid_Badge_Carries_The_Validation_Reason()
    {
        var lines = PlanPanelContent.Lines(PlanState.Invalid, "Unknown transformer 'bogus'", null);
        Assert.Equal("✗ invalid — Unknown transformer 'bogus'", lines.Last());
    }

    [Fact]
    public void A_Failed_Badge_Carries_The_Execution_Reason()
    {
        var lines = PlanPanelContent.Lines(PlanState.Failed, "connection refused", null);
        Assert.Equal("✗ failed — connection refused", lines.Last());
    }

    [Fact]
    public void A_Failure_Badge_Without_A_Reason_Points_At_The_Transcript()
    {
        var lines = PlanPanelContent.Lines(PlanState.Failed, null, null);
        Assert.Equal("✗ failed — see transcript", lines.Last());
    }

    [Fact]
    public void The_Applied_Badge_States_The_Write_Happened()
    {
        var lines = PlanPanelContent.Lines(PlanState.Applied, null,
            Topo(Branch("main", input: "csv:in.csv", output: "pg:out")));
        Assert.Equal("✓ applied — data written", lines.Last());
    }

    [Fact]
    public void A_Long_Reason_Is_Clipped_And_Flattened()
    {
        var noisy = "line one\nline two " + new string('x', 200);
        var lines = PlanPanelContent.Lines(PlanState.Invalid, noisy, null);

        var badge = lines.Last();
        Assert.DoesNotContain("\n", badge);
        Assert.EndsWith("…", badge);
    }
}
