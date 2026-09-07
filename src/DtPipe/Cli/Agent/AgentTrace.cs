using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Agent;

/// <summary>
/// A session's diagnostic record, one JSON object per line, written as it happens.
///
/// <para>
/// It exists to answer one question a passing or failing exit code cannot: <em>when a model fails a
/// mission, what would have to change here for it to succeed?</em> That question has four possible
/// answers — the system prompt, a tool's description, a tool's behaviour, or nothing (the model was
/// simply not up to it) — and telling them apart needs what the model was given, not only what it
/// did. So the trace carries the system prompt actually selected and the tool catalogue as it was
/// offered, alongside the steps. A trace without those two records the symptom and loses the cause.
/// </para>
///
/// <para>
/// Every line is flushed, because the runs worth reading are the ones that end badly — a trace that
/// buffered would lose the last thing that happened, which is the thing being looked for.
/// </para>
///
/// <para>
/// <b>It is not a gate and must not become one.</b> A fail-closed criterion over an LLM loop needs
/// an attributable signal, which is a separate and unsettled question; a diagnostic read by a person
/// is useful without being decidable. Nothing here returns a verdict.
/// </para>
///
/// <para>
/// <b>Contents are as the model saw them.</b> Tool arguments and results are written verbatim, past
/// a pass of <see cref="ConnectionStringSanitizer"/> that blanks the shapes it recognises — a
/// <c>password=</c> pair, credentials in a URI. It recognises shapes, not secrets: a credential in
/// any other form reaches the file. The trace is off unless asked for, and the header says so.
/// </para>
/// </summary>
public sealed class AgentTrace : IDisposable
{
    private static readonly JsonSerializerOptions Compact = new() { WriteIndented = false };

    private readonly object _gate = new();
    private readonly StreamWriter _out;
    private int _turn;

    private AgentTrace(StreamWriter writer) => _out = writer;

    /// <summary>The file this session is writing to — printed once so the reader can find it.</summary>
    public string Path { get; private init; } = string.Empty;

    /// <summary>
    /// Opens a trace at <paramref name="destination"/>, or returns null when none was asked for. A
    /// destination that names a directory gets a timestamped file inside it, which is what a
    /// campaign of sessions wants; anything else is taken as the file itself. Returns null rather
    /// than throwing when the path cannot be written: a diagnostic that aborts the run it was
    /// meant to diagnose is worse than no diagnostic.
    /// </summary>
    public static AgentTrace? Open(string? destination, out string? failure)
    {
        failure = null;
        if (string.IsNullOrWhiteSpace(destination)) return null;

        try
        {
            var path = destination!;
            if (Directory.Exists(path) || path.EndsWith(System.IO.Path.DirectorySeparatorChar))
            {
                Directory.CreateDirectory(path);
                path = System.IO.Path.Combine(path, $"agent-{DateTime.UtcNow:yyyyMMdd-HHmmss}.jsonl");
            }
            else
            {
                var parent = System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(path));
                if (!string.IsNullOrEmpty(parent)) Directory.CreateDirectory(parent);
            }

            var writer = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                AutoFlush = true,
            };
            return new AgentTrace(writer) { Path = path };
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            failure = ex.Message;
            return null;
        }
    }

    /// <summary>How the run was launched. Written once, first.</summary>
    public void Session(string model, string endpoint, AgentMode mode, AgentOptions opts, int maxIterations)
        => Write(new Dictionary<string, object?>
        {
            ["kind"] = "session",
            ["dtpipe"] = typeof(AgentTrace).Assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
            ["model"] = model,
            ["endpoint"] = Safe(endpoint),
            ["mode"] = mode.ToString().ToLowerInvariant(),
            ["apply"] = opts.Apply,
            ["allowDestructive"] = opts.AllowDestructive,
            ["allowNetwork"] = opts.AllowNetwork,
            ["detail"] = opts.Detail.ToString().ToLowerInvariant(),
            ["maxIterations"] = maxIterations,
            ["warning"] = "Tool arguments and results are recorded as the model saw them; "
                        + "known credential shapes are blanked, others are not.",
        });

    /// <summary>The role prompt the mode selected, in full — a prompt fix is only attributable against it.</summary>
    public void SystemPrompt(AgentMode mode, string text)
        => Write(new Dictionary<string, object?>
        {
            ["kind"] = "system-prompt",
            ["mode"] = mode.ToString().ToLowerInvariant(),
            ["text"] = text,
        });

    /// <summary>
    /// The tools as offered for this turn, with the descriptions the model read. Without this a
    /// wrong tool call cannot be told apart from a tool that was never offered, or one whose
    /// description sent the model elsewhere.
    /// </summary>
    public void Catalogue(AgentMode mode, IReadOnlyList<ToolDefinition> tools)
        => Write(new Dictionary<string, object?>
        {
            ["kind"] = "tools",
            ["mode"] = mode.ToString().ToLowerInvariant(),
            ["tools"] = tools.Select(t => new Dictionary<string, object?>
            {
                ["name"] = t.Name,
                ["description"] = t.Description,
            }).ToList(),
        });

    /// <summary>A turn opening on what the user asked for.</summary>
    public void Turn(string prompt)
    {
        lock (_gate) _turn++;
        Write(new Dictionary<string, object?> { ["kind"] = "turn", ["n"] = _turn, ["prompt"] = Safe(prompt) });
    }

    /// <summary>One step of the loop, as it was recorded on the trajectory.</summary>
    public void Step(TrajectoryStep step)
        => Write(new Dictionary<string, object?>
        {
            ["kind"] = "step",
            ["turn"] = _turn,
            ["iteration"] = step.Iteration,
            ["reasoning"] = Safe(step.Reasoning),
            ["thinking"] = Safe(step.Thinking),
            ["tool"] = step.ToolName,
            ["args"] = Safe(step.ToolArgs),
            ["result"] = Safe(step.ToolResult),
            ["isError"] = step.IsError,
            ["promptTokens"] = step.Usage?.PromptTokens,
            ["completionTokens"] = step.Usage?.CompletionTokens,
        });

    /// <summary>What the human watching made of it — the only ground truth an interactive run has.</summary>
    public void Note(string text)
        => Write(new Dictionary<string, object?> { ["kind"] = "note", ["turn"] = _turn, ["text"] = Safe(text) });

    /// <summary>How the turn ended.</summary>
    public void Verdict(TurnSummaryModel summary)
        => Write(new Dictionary<string, object?>
        {
            ["kind"] = "verdict",
            ["turn"] = _turn,
            ["outcome"] = summary.Outcome.ToString(),
            ["status"] = summary.Status.ToString(),
            ["iterations"] = summary.Iterations,
            ["seconds"] = Math.Round(summary.Duration.TotalSeconds, 2),
            ["tokens"] = summary.Tokens,
            ["toolCalls"] = summary.TotalToolCalls,
            ["producedPlan"] = summary.ProducedPlan,
            ["question"] = Safe(summary.Question?.Text),
            // Beside the question, not folded into it: a wrong answer cannot be told apart from a
            // question that offered the wrong choices without seeing what was offered.
            ["questionOptions"] = summary.Question?.Options.Select(Safe).ToList(),
        });

    private void Write(Dictionary<string, object?> record)
    {
        record["at"] = DateTime.UtcNow.ToString("O");
        try
        {
            var line = JsonSerializer.Serialize(record, Compact);
            lock (_gate) _out.WriteLine(line);
        }
        catch (Exception)
        {
            // A trace must never take the run down with it.
        }
    }

    private static string? Safe(string? text) => text is null ? null : ConnectionStringSanitizer.Sanitize(text);

    public void Dispose()
    {
        try { _out.Dispose(); } catch (Exception) { /* already gone */ }
    }
}
