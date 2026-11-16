using System;
using System.Linq;
using System.Text.Json;
using DtPipe.Cli.Mcp;

namespace DtPipe.Cli.Agent;

/// <summary>How far the plan the agent is building has got. A badge, not a verdict.</summary>
internal enum PlanState
{
    /// <summary>No plan YAML has been seen yet.</summary>
    None,

    /// <summary>A plan YAML exists but has not been validated (or was changed since it last was).</summary>
    Drafted,

    /// <summary><c>validate-yaml-job</c> accepted the current plan.</summary>
    Validated,

    /// <summary><c>validate-yaml-job</c> rejected the current plan.</summary>
    Invalid,

    /// <summary>A sample ran the plan with the writer neutralised: <c>dry-run</c>, or <c>execute-yaml-job</c> with <c>apply=false</c>.</summary>
    DryRunOk,

    /// <summary>A real write ran: <c>execute-yaml-job</c> with <c>apply=true</c>.</summary>
    Applied,

    /// <summary>A sample, a save or a run failed, on the safety gate or during execution.</summary>
    Failed,

    /// <summary><c>save-job</c> wrote the plan into the folder, the person having confirmed it.</summary>
    Saved,
}

/// <summary>
/// Tracks the state of the pipeline the agent is designing, from the tool calls it makes: a
/// <c>yamlContent</c> argument drafts (or re-drafts) the plan, <c>validate-yaml-job</c> and
/// <c>execute-yaml-job</c> results move it forward. Pure — no console, no LLM — so a scripted
/// sequence of outcomes is asserted directly. It is the source the plan panel reads.
/// </summary>
internal sealed class PlanProgress
{
    private const int MessageCap = 400;

    /// <summary>The current badge.</summary>
    public PlanState State { get; private set; } = PlanState.None;

    /// <summary>The plan YAML currently tracked, or null before the first draft.</summary>
    public string? Yaml { get; private set; }

    /// <summary>The last error / status detail (from a failed validate or execute), or null.</summary>
    public string? Message { get; private set; }

    /// <summary>
    /// A fresh plan YAML was captured from a <c>yamlContent</c> tool-call argument. An unchanged
    /// YAML is a no-op; a different one re-drafts (a modified plan is no longer validated).
    /// </summary>
    public void OnPlanUpdated(string? yaml)
    {
        if (string.IsNullOrWhiteSpace(yaml)) return;
        if (State != PlanState.None && string.Equals(yaml, Yaml, StringComparison.Ordinal)) return;

        Yaml = yaml;
        State = PlanState.Drafted;
        Message = null;
    }

    /// <summary>The tool whose result moves a drafted plan to validated or invalid.</summary>
    internal const string ValidateTool = "validate-yaml-job";

    /// <summary>The tool whose result moves a plan to dry-run, applied or failed.</summary>
    internal const string ExecuteTool = "execute-yaml-job";

    /// <summary>The sample and the save of the studio's agent, whose results move the plan too.</summary>
    internal const string DryRunTool = "dry-run";
    internal const string SaveTool = "save-job";

    /// <summary>
    /// A tool call finished. Only the tools named here move the plan state, whatever the spelling the model used; every
    /// other tool is ignored. Ignored entirely until a plan has been drafted.
    /// </summary>
    public void OnToolResult(string toolName, bool isError, string? resultJson)
    {
        if (State == PlanState.None) return;

        switch (McpToolReflector.Normalise(toolName))
        {
            case var n when n == McpToolReflector.Normalise(DryRunTool):
                if (isError) { State = PlanState.Failed; Message = ExtractMessage(resultJson); }
                else if (State is not (PlanState.Applied or PlanState.Saved)) { State = PlanState.DryRunOk; Message = null; }
                break;

            case var n when n == McpToolReflector.Normalise(SaveTool):
                if (isError) { State = PlanState.Failed; Message = ExtractMessage(resultJson); }
                else if (Has(resultJson, "saved")) { State = PlanState.Saved; Message = null; }
                break;

            case var n when n == McpToolReflector.Normalise(ValidateTool):
                if (isError)
                {
                    State = PlanState.Invalid;
                    Message = ExtractMessage(resultJson);
                }
                else
                {
                    State = PlanState.Validated;
                    Message = null;
                }
                break;

            case var n when n == McpToolReflector.Normalise(ExecuteTool):
                if (isError)
                {
                    State = PlanState.Failed;
                    Message = ExtractMessage(resultJson);
                }
                else if (ReadApplied(resultJson))
                {
                    State = PlanState.Applied;
                    Message = null;
                }
                else if (State != PlanState.Applied)
                {
                    // A dry-run after a real write does not walk the badge back to DryRunOk.
                    State = PlanState.DryRunOk;
                    Message = null;
                }
                break;
        }
    }

    private static bool Has(string? resultJson, string property)
    {
        try
        {
            using var doc = JsonDocument.Parse(resultJson ?? "");
            return doc.RootElement.ValueKind == JsonValueKind.Object && doc.RootElement.TryGetProperty(property, out var v) && v.ValueKind != JsonValueKind.Null;
        }
        catch (JsonException) { return false; }
    }

    private static int? Code(string? resultJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(resultJson ?? "");
            return doc.RootElement.TryGetProperty("code", out var c) && c.TryGetInt32(out var code) ? code : null;
        }
        catch (JsonException) { return null; }
    }

    /// <summary>Whether an <c>execute-yaml-job</c> result reports a real write (<c>applied: true</c> or <c>mode: "write"</c>).</summary>
    private static bool ReadApplied(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson)) return false;
        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return false;

            if (root.TryGetProperty("applied", out var a) &&
                (a.ValueKind == JsonValueKind.True || a.ValueKind == JsonValueKind.False))
                return a.GetBoolean();

            return root.TryGetProperty("mode", out var m)
                && m.ValueKind == JsonValueKind.String
                && string.Equals(m.GetString(), "write", StringComparison.Ordinal);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>Pulls a short human-readable reason out of a failed tool result.</summary>
    private static string? ExtractMessage(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson)) return null;
        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return Clip(resultJson);

            if (root.TryGetProperty("errors", out var errs) && errs.ValueKind == JsonValueKind.Array)
            {
                var joined = string.Join("; ", errs.EnumerateArray()
                    .Where(e => e.ValueKind == JsonValueKind.String)
                    .Select(e => e.GetString()));
                if (!string.IsNullOrWhiteSpace(joined)) return Clip(joined);
            }

            if (root.TryGetProperty("violation", out var v) && v.ValueKind == JsonValueKind.Array)
            {
                var joined = string.Join("; ", v.EnumerateArray()
                    .Where(e => e.ValueKind == JsonValueKind.String)
                    .Select(e => e.GetString()));
                if (!string.IsNullOrWhiteSpace(joined)) return Clip(joined);
            }

            foreach (var key in new[] { "error", "message" })
            {
                if (root.TryGetProperty(key, out var s) && s.ValueKind == JsonValueKind.String)
                {
                    var val = s.GetString();
                    if (!string.IsNullOrWhiteSpace(val)) return Clip(val);
                }
            }

            return null;
        }
        catch (JsonException)
        {
            return Clip(resultJson);
        }
    }

    private static string Clip(string? s)
    {
        s = (s ?? string.Empty).Trim();
        return s.Length > MessageCap ? s[..MessageCap] + "…" : s;
    }
}
