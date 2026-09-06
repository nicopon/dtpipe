using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Agent;

/// <summary>The plan panel's state, read on the UI thread from <see cref="Tui.TuiTurnView.PlanSnapshot"/>.</summary>
internal readonly record struct PlanView(PlanState State, string? Message, DagTopology? Topology);

/// <summary>
/// The plan/DAG panel as UI-agnostic lines: one row per branch, every stage of it in order
/// (<c>alias: source → transformer → [processor] → output</c>), plus a status badge derived from
/// <see cref="PlanProgress"/>. Pure — no toolkit — so the panel is asserted on this directly.
/// Connection strings are sanitised: this is on screen while the run goes.
/// </summary>
internal static class PlanPanelContent
{
    public static IReadOnlyList<string> Lines(PlanView plan) => Lines(plan.State, plan.Message, plan.Topology);

    public static IReadOnlyList<string> Lines(PlanState state, string? message, DagTopology? topology)
    {
        var lines = new List<string>();

        if (topology is { Branches.Count: > 0 })
            lines.AddRange(topology.Branches.Select(BranchLine));
        else if (state != PlanState.None)
            lines.Add("(plan YAML not parsed yet)");

        lines.Add(Badge(state, message));
        return lines;
    }

    private static string BranchLine(BranchTopology b)
    {
        var stages = new List<string>
        {
            b.From.Count > 0 ? string.Join(",", b.From) : Endpoint(b.Input) ?? "(channel)"
        };
        stages.AddRange(b.Transformers);
        if (!string.IsNullOrEmpty(b.Processor)) stages.Add($"[{b.Processor}]");
        stages.Add(Endpoint(b.Output) ?? "(none)");

        var line = $"{b.Alias}: {string.Join(" → ", stages)}";
        if (b.Ref.Count > 0) line += $"  (ref: {string.Join(",", b.Ref)})";
        return line;
    }

    private static string Badge(PlanState state, string? message) => state switch
    {
        PlanState.None => "○ no plan yet",
        PlanState.Drafted => "◐ drafted — not validated",
        PlanState.Validated => "✓ validated",
        PlanState.Invalid => $"✗ invalid — {Reason(message)}",
        PlanState.DryRunOk => "✓ dry-run ok — nothing written",
        PlanState.Applied => "✓ applied — data written",
        PlanState.Failed => $"✗ failed — {Reason(message)}",
        _ => state.ToString(),
    };

    private static string Reason(string? message)
    {
        if (string.IsNullOrWhiteSpace(message)) return "see transcript";
        var one = message.Replace('\n', ' ').Replace('\r', ' ').Trim();
        return one.Length > 120 ? one[..120] + "…" : one;
    }

    private static string? Endpoint(string? connectionString) =>
        string.IsNullOrEmpty(connectionString) ? null : ConnectionStringSanitizer.Sanitize(connectionString);
}
