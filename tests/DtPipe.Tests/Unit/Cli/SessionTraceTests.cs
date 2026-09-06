using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The session's diagnostic record. It exists to make a failed run attributable — to the role
/// prompt, to a tool's description, to a tool's behaviour, or to the model — so the two records
/// that carry the cause rather than the symptom (the prompt in force, the catalogue as offered)
/// matter as much as the steps.
/// </summary>
public class SessionTraceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dtpipe-trace-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch (Exception) { /* best effort */ }
    }

    private static IReadOnlyList<JsonElement> Read(string path) =>
        File.ReadAllLines(path).Where(l => l.Length > 0)
            .Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();

    private static readonly ToolDefinition[] Catalogue =
    [
        new("inspect", "Inspect a source's schema.", JsonDocument.Parse("{}").RootElement.Clone()),
    ];

    [Fact]
    public void No_Destination_Means_No_Trace()
    {
        Assert.Null(AgentTrace.Open(null, out var failure));
        Assert.Null(failure);
        Assert.Null(AgentTrace.Open("   ", out _));
    }

    /// <summary>A directory is a campaign: each session lands in its own file inside it.</summary>
    [Fact]
    public void A_Directory_Destination_Gets_A_File_Of_Its_Own()
    {
        Directory.CreateDirectory(_dir);
        using var trace = AgentTrace.Open(_dir, out var failure);

        Assert.Null(failure);
        Assert.NotNull(trace);
        Assert.Equal(_dir, Path.GetDirectoryName(trace!.Path));
        Assert.EndsWith(".jsonl", trace.Path);
    }

    /// <summary>
    /// Every record the analysis needs, in one session. The prompt and the catalogue are the two
    /// that turn "the model called a tool that does not exist" into "the tool was never offered"
    /// or "its description sent the model elsewhere".
    /// </summary>
    [Fact]
    public void A_Session_Records_What_The_Model_Was_Given_As_Well_As_What_It_Did()
    {
        var path = Path.Combine(_dir, "run.jsonl");
        using (var trace = AgentTrace.Open(path, out _))
        {
            Assert.NotNull(trace);
            trace!.Session("gpt-oss:20b", "http://localhost:11434", AgentMode.Plan, new AgentOptions(), 25);
            trace.SystemPrompt(AgentMode.Plan, "you are a planner");
            trace.Catalogue(AgentMode.Plan, Catalogue);
            trace.Turn("build me a pipeline");
            trace.Step(new TrajectoryStep
            {
                Iteration = 1, Reasoning = "look first",
                ToolName = "preview_data", ToolArgs = "{}",
                ToolResult = "{\"code\":-32602,\"message\":\"Unknown tool\"}", IsError = true,
            });
            trace.Note("it guessed a tool name that does not exist");
            trace.Verdict(new TurnSummaryModel(TurnOutcome.MaxIterationsReached, 10, TimeSpan.FromSeconds(48),
                new Dictionary<string, int> { ["inspect"] = 3 }, Tokens: 1991));
        }

        var kinds = Read(path).Select(r => r.GetProperty("kind").GetString()).ToArray();
        Assert.Equal(new[] { "session", "system-prompt", "tools", "turn", "step", "note", "verdict" }, kinds);

        var records = Read(path);
        Assert.Equal("you are a planner", records[1].GetProperty("text").GetString());
        Assert.Equal("inspect", records[2].GetProperty("tools")[0].GetProperty("name").GetString());
        Assert.Contains("Inspect a source", records[2].GetProperty("tools")[0].GetProperty("description").GetString());
        Assert.True(records[4].GetProperty("isError").GetBoolean());
        Assert.Equal(1, records[4].GetProperty("turn").GetInt32());     // the step knows its turn
        Assert.Equal("MaxIterationsReached", records[6].GetProperty("outcome").GetString());
        Assert.Equal(1991, records[6].GetProperty("tokens").GetInt64());
        Assert.All(records, r => Assert.True(r.TryGetProperty("at", out _)));
    }

    /// <summary>
    /// Credentials the sanitiser recognises are blanked wherever they appear. It recognises shapes,
    /// not secrets — which is why the session header says so rather than claiming the file is safe.
    /// </summary>
    [Fact]
    public void Known_Credential_Shapes_Are_Blanked()
    {
        var path = Path.Combine(_dir, "secrets.jsonl");
        using (var trace = AgentTrace.Open(path, out _))
        {
            trace!.Turn("read from Host=db;Username=app;Password=hunter2 please");
            trace.Step(new TrajectoryStep
            {
                Iteration = 1, Reasoning = "connecting",
                ToolArgs = "postgres://app:hunter2@db:5432/sales", ToolName = "inspect",
            });
        }

        var text = File.ReadAllText(path);
        Assert.DoesNotContain("hunter2", text);
        Assert.Contains("app", text);          // only the credential goes, not the context
    }

    /// <summary>A diagnostic that takes the run down with it has failed at its own job.</summary>
    [Fact]
    public void An_Unwritable_Destination_Is_Reported_And_Not_Thrown()
    {
        var trace = AgentTrace.Open(Path.Combine(_dir, "run.jsonl", "deeper.jsonl"), out var failure);

        if (trace is null) Assert.NotNull(failure);
        else trace.Dispose();
    }
}
