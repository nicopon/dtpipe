using System;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Cli.Agent;

/// <summary>
/// A cached "fact" produced by a tool result that should survive conversation compaction
/// (F4 — non-destructive context). Facts are keyed by a stable key derived from the inputs
/// that produced them (e.g. "inspect csv:invoices.csv") so they can be selectively reloaded
/// into the compacted message window instead of being discarded.
/// </summary>
public sealed class Fact
 {
    public string Key { get; init; } = string.Empty;

       /// <summary>Tool that produced the fact; see AgentExecutor.FactProducingTools for the set.</summary>
    public string ToolName { get; init; } = string.Empty;

       /// <summary>The (possibly truncated) tool result payload.</summary>
    public string Content { get; init; } = string.Empty;

       /// <summary>When true, the fact records an error/warning rather than a successful fact.</summary>
    public bool IsError { get; init; }

    public DateTime Timestamp { get; init; } = DateTime.Now;
}

/// <summary>
/// Caches fact tool results so that compaction does not lose inspected schemas, sample rows,
/// and recent errors. Kept independent of the conversation window so the full journal stays in
/// <see cref="AgentTrajectory"/>. KISS: no second LLM call is required to preserve context.
/// </summary>
public sealed class AgentContextStore
 {
    private const int MaxFacts = 64;
    private const int MaxFactContentChars = 4000;

    private readonly Dictionary<string, Fact> _facts = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();

       /// <summary>
       /// Record a fact produced by a tool call. The result is capped to keep the compacted
       /// window bounded, but is never evicted by message compaction.
       /// </summary>
    public void RecordFact(string key, string toolName, string content, bool isError = false)
      {
        if (string.IsNullOrWhiteSpace(key)) return;

        var capped = content.Length > MaxFactContentChars
             ? content[..MaxFactContentChars] + "\n…[truncated]"
             : content;

        lock (_lock)
         {
            // Most recent write wins for a given key.
            _facts[key] = new Fact
             {
                Key = key,
                ToolName = toolName,
                Content = capped,
                IsError = isError,
                Timestamp = DateTime.Now
             };

             // Enforce a hard cap on the number of distinct facts (drop oldest).
            if (_facts.Count > MaxFacts)
             {
                var oldest = _facts.Values
                     .OrderBy(f => f.Timestamp)
                     .Take(_facts.Count - MaxFacts)
                     .ToList();
                foreach (var o in oldest)
                 {
                    _facts.Remove(o.Key);
                 }
             }
         }
      }

       /// <summary>Return all cached facts, newest first.</summary>
    public IReadOnlyList<Fact> GetFacts()
      {
        lock (_lock)
         {
            return _facts.Values.OrderByDescending(f => f.Timestamp).ToList();
         }
      }

       /// <summary>Return a compact, human-readable FACTS block for the compacted message window.</summary>
    public string BuildFactsBlock()
      {
        var facts = GetFacts();
        if (facts.Count == 0)
         {
            return string.Empty;
         }

        var sb = new System.Text.StringBuilder();
        sb.AppendLine("[FACTS — preserved across compaction]");
        foreach (var f in facts)
         {
            string tag = f.IsError ? "ERROR" : "FACT";
            sb.AppendLine($"  - ({tag}) {f.ToolName} [{f.Key}]:");
            foreach (var line in f.Content.Split('\n'))
             {
                sb.AppendLine($"      {line}");
             }
         }
        return sb.ToString().TrimEnd();
      }
 }