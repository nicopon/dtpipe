using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The full-screen turn view only mutates a
/// <see cref="TranscriptLog"/> — it marshals nothing and touches no toolkit type, which is why it
/// is asserted here without a terminal. The surface reads the log on a timer; that inversion is
/// what keeps a repaint off the model's token rate.
/// </summary>
public class TuiTurnViewTests
{
    private static LlmResponse ToolStep(string content, string tool)
    {
        var calls = new List<ToolCall> { new("c1", tool, System.Text.Json.JsonDocument.Parse("{}").RootElement.Clone()) };
        return new LlmResponse(new ChatMessage("assistant", content, null, calls), true, null,
            new LlmUsage(10, 42, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public void A_Digest_Becomes_A_Transcript_Entry()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        view.Digest(1, 25, TimeSpan.FromSeconds(3), ToolStep("INTENT: inspect the source", "inspect"), AgentDetailLevel.Compact);

        var entry = Assert.Single(log.Entries);
        Assert.Equal(TranscriptEntryKind.Digest, entry.Kind);
        Assert.Contains("Step 1/25", entry.Text);
        Assert.Contains("INTENT: inspect the source", entry.Text);
    }

    [Fact]
    public void Full_Detail_Adds_The_Chain_Of_Thought_As_Its_Own_Entry()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        var response = new LlmResponse(ToolStep("INTENT: go", "inspect").Message, true, null, null, "私 thinking here");

        view.Digest(1, 25, TimeSpan.FromSeconds(1), response, AgentDetailLevel.Full);

        Assert.Equal(2, log.Entries.Count);
        Assert.Equal(TranscriptEntryKind.Thinking, log.Entries[1].Kind);
        Assert.Contains("thinking here", log.Entries[1].Text);
    }

    [Fact]
    public void Compact_Detail_Keeps_The_Chain_Of_Thought_Out()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        var response = new LlmResponse(ToolStep("INTENT: go", "inspect").Message, true, null, null, "private reasoning");

        view.Digest(1, 25, TimeSpan.FromSeconds(1), response, AgentDetailLevel.Compact);

        Assert.Single(log.Entries);
        Assert.DoesNotContain(log.Entries, e => e.Kind == TranscriptEntryKind.Thinking);
    }

    [Fact]
    public void Tool_Results_And_Answers_Become_Entries_Of_Their_Own_Kind()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        view.ToolResult("inspect", "{\"cols\":8}", isError: false);
        view.ToolResult("execute-yaml-job", "{\"success\":false}", isError: true);
        view.AgentResponse("here is the validated plan");

        Assert.Collection(log.Entries,
            e => { Assert.Equal(TranscriptEntryKind.ToolResult, e.Kind); Assert.False(e.IsError); },
            e => { Assert.Equal(TranscriptEntryKind.ToolResult, e.Kind); Assert.True(e.IsError); },
            e => { Assert.Equal(TranscriptEntryKind.AgentResponse, e.Kind); Assert.Contains("validated plan", e.Text); });
    }

    [Fact]
    public void An_Empty_Answer_Adds_Nothing()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        view.AgentResponse("   ");

        Assert.Empty(log.Entries);
    }

    [Fact]
    public async Task A_Streaming_Step_Exposes_A_Live_Tail_And_Clears_It_At_The_End()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        string? tailDuringStream = null;

        var response = await view.StreamingStepAsync(1, 25, AgentDetailLevel.Compact, obs =>
        {
            obs.OnThinking("weighing the options");
            obs.OnContent("INTENT: inspect");
            tailDuringStream = view.LiveTailPlain();
            return Task.FromResult(ToolStep("INTENT: inspect", "inspect"));
        });

        Assert.NotNull(response);
        Assert.NotNull(tailDuringStream);
        Assert.Contains("weighing the options", tailDuringStream);
        // The tail is a live view, never part of the permanent record.
        Assert.Null(view.LiveTailPlain());
        Assert.Null(log.LiveTail);
        Assert.Contains(log.Entries, e => e.Kind == TranscriptEntryKind.Digest);
    }

    [Fact]
    public async Task A_Streaming_Step_That_Throws_Still_Clears_The_Live_Tail()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            view.StreamingStepAsync(1, 25, AgentDetailLevel.Compact, obs =>
            {
                obs.OnContent("half a thought");
                throw new OperationCanceledException();
            }));

        Assert.Null(view.LiveTailPlain());
        Assert.Null(log.LiveTail);
    }

    [Fact]
    public async Task A_Blocking_Step_Shows_And_Clears_A_Placeholder_Tail()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        string? seen = null;

        await view.BlockingStepAsync(4, _ =>
        {
            seen = log.LiveTail;
            return Task.FromResult(ToolStep("INTENT: go", "inspect"));
        }, CancellationToken.None);

        Assert.Contains("Step 4", seen);
        Assert.Null(log.LiveTail);
    }

    [Fact]
    public void The_Log_Version_Moves_On_Every_Mutation_So_A_Poll_Can_Skip_Work()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        long start = log.Version;
        view.ToolResult("inspect", "{}", false);
        long afterEntry = log.Version;
        log.LiveTail = "streaming…";
        long afterTail = log.Version;

        Assert.True(afterEntry > start);
        Assert.True(afterTail > afterEntry);
        // Reading changes nothing — an unchanged version means an unchanged transcript.
        Assert.Equal(afterTail, log.Version);
    }

    [Fact]
    public void Re_Writing_The_Same_Live_Tail_Is_Not_A_Change()
    {
        // The repaint poll assigns the tail every tick. If that counted as a mutation the view
        // would be rebuilt ten times a second between steps, for nothing.
        var log = new TranscriptLog { LiveTail = "· Step 1 — thinking…" };

        long before = log.Version;
        log.LiveTail = "· Step 1 — thinking…";
        Assert.Equal(before, log.Version);

        log.LiveTail = "· Step 1 — thinking… 2s";
        Assert.True(log.Version > before);

        long afterText = log.Version;
        log.LiveTail = null;
        long afterClear = log.Version;
        Assert.True(afterClear > afterText);

        log.LiveTail = null;
        Assert.Equal(afterClear, log.Version);
    }

    [Fact]
    public void Plain_Lines_Carry_The_Committed_Text_Then_The_Live_Tail_And_Stay_Bounded()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        for (int i = 0; i < 40; i++)
            view.ToolResult("inspect", $"result {i}", false);
        log.LiveTail = "· Step 41 — thinking…";

        var all = log.PlainLines();
        Assert.Contains("· Step 41 — thinking…", all.Last());

        var bounded = log.PlainLines(maxLines: 5);
        Assert.Equal(5, bounded.Count);
        Assert.Contains("· Step 41 — thinking…", bounded.Last());
    }

    [Fact]
    public void Markup_Lines_Are_The_Scrollback_Replay_And_Exclude_The_Live_Tail()
    {
        var log = new TranscriptLog();
        var view = new TuiTurnView(log);

        view.ToolResult("inspect", "{\"cols\":8}", false);
        log.LiveTail = "half a streamed step";

        var replay = log.MarkupLines();

        Assert.Single(replay);
        Assert.Contains("inspect", replay[0]);
        Assert.DoesNotContain(replay, l => l.Contains("half a streamed step"));
    }
}
