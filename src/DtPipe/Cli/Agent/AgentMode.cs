using System.Collections.Generic;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Operating mode of the agent. Controls the available tool set and whether the LLM
/// is allowed to drive real execution.
/// </summary>
public enum AgentMode
 {
    /// <summary>
    /// The LLM plans and validates a pipeline (produces a validated <see cref="AgentPlan"/>).
    /// Execution is a deterministic step run by the engine, never by the LLM.
    /// <c>execute-yaml-job</c> is NOT in the tool allow-list.
    /// </summary>
    Plan,

    /// <summary>
    /// The LLM may drive execution end-to-end. Execution is gated by the guardrails
    /// (dry-run by default, approval gate, SQL safety policy).
    /// </summary>
    Execute,

    /// <summary>
    /// Combines planning and execution. The LLM plans, validates, then executes through
    /// the guardrails (still dry-run by default unless <c>apply</c> + approval are granted).
    /// </summary>
    Autonomous
}

/// <summary>
/// Configuration for a single agent run. All fields are optional with safe defaults so
/// that <c>dtpipe agent</c> with no flags is the safest behavior:
/// mode = Plan, dry-run, deterministic (temperature 0, seed 0).
/// </summary>
public sealed class AgentOptions
 {
      /// <summary>Ceiling for a single LLM call. Past this the call is reported as a failed call,
      /// not left to stall until the HTTP stack's own default fires (which surfaces as a bare
      /// cancellation and is easily mistaken for a user Ctrl-C). Threaded to the LLM client at
      /// construction; <c>--llm-timeout</c> overrides it.</summary>
    public static readonly System.TimeSpan DefaultLlmTimeout = System.TimeSpan.FromSeconds(300);

    public AgentMode Mode { get; init; } = AgentMode.Plan;

      /// <summary>Default model context window requested from the provider (Ollama <c>num_ctx</c>).
      /// The agent prompt carries the tool catalogue plus a growing transcript, so the provider's
      /// own small default (4k) is not enough. Raise it with <c>--num-ctx</c> if a reasoning model
      /// still loops; going much higher mainly costs prefill time and VRAM.</summary>
    public const int DefaultNumCtx = 16384;

      /// <summary>
      /// Sampling temperature. 0 is greedy decoding, which is where a weak or heavily quantized
      /// model degenerates: dtpipe's own repetition guard tells the user to raise it, and a
      /// measurement run on a 12B local model returned empty responses at 0 and complete plans at
      /// 1. A default whose own error messages advise leaving it is not a default.
      ///
      /// <para>
      /// Reproducibility does not go with it: <see cref="Seed"/> still fixes the sampling, so the
      /// same prompt and seed give the same run. What is given up is greedy decoding, not the
      /// ability to replay — and <c>--temperature 0</c> is still there for the determinism report.
      /// </para>
      /// </summary>
    public double Temperature { get; init; } = 1.0;

      /// <summary>Model context window to request from the provider.</summary>
    public int NumCtx { get; init; } = DefaultNumCtx;

      /// <summary>Disable token streaming and its live view; fall back to a single blocking call.</summary>
    public bool NoStream { get; init; } = false;

      /// <summary>How much of each step stays in scrollback once its live region collapses.
      /// <c>--detail full</c> (and <c>DEBUG=1</c>, and the <c>--show-thinking</c> alias) keeps the
      /// whole chain of thought; the default keeps a trace line plus the model's stated intent.</summary>
    public AgentDetailLevel Detail { get; init; } = AgentDetailLevel.Compact;

      /// <summary>Optional fixed seed for reproducible sampling. Null => provider picks its own.</summary>
    public int? Seed { get; init; } = 0;

      /// <summary>Number of replications of the validated plan for determinism/variance measurement.</summary>
    public int Repeat { get; init; } = 1;

      /// <summary>When true, independent tool calls are executed sequentially instead of in parallel.</summary>
    public bool Sequential { get; init; } = false;

      /// <summary>Optional rolling-summary compaction (a second LLM call). Off by default (KISS).</summary>
    public bool RollingSummary { get; init; } = false;

      /// <summary>Allow destructive SQL verbs (DROP/DELETE/TRUNCATE/...). Default deny.</summary>
    public bool AllowDestructive { get; init; } = false;

      /// <summary>Allow network access in SQL (LOAD httpfs/azure, remote read_parquet). Default deny.</summary>
    public bool AllowNetwork { get; init; } = false;

      /// <summary>Whether execute-yaml-job performs a real write. Default false (dry-run).</summary>
    public bool Apply { get; init; } = false;

      /// <summary>Restore the legacy monolithic ReAct behavior (single tool call per iteration).</summary>
    public bool LegacyAgent { get; init; } = false;

      /// <summary>
      /// Opt out of the full-screen surface and keep the scrollback path on a real interactive
      /// terminal. The surface is the default there; a pipe, a redirect or <c>--no-stream</c>
      /// already keeps the sequential path regardless of this flag — that is what CI asserts
      /// against, and this option only matters at a real terminal.
      /// </summary>
    public bool NoTui { get; init; } = false;
}

/// <summary>
/// A validated pipeline plan produced by the planner. Execution of this plan is a
/// deterministic engine step — the LLM no longer drives execution.
/// </summary>
public sealed class AgentPlan
 {
     /// <summary>The source-of-truth YAML configuration (from the <c>yamlContent</c> tool argument).</summary>
    public string Yaml { get; init; } = string.Empty;

      /// <summary>Parsed DAG definition backing the plan.</summary>
    public DtPipe.Core.Pipelines.Dag.JobDagDefinition? DagDefinition { get; init; }

      /// <summary>Validation report (empty list => valid).</summary>
    public List<string> ValidationReport { get; init; } = new();

      /// <summary>Operating mode the plan was produced under.</summary>
    public AgentMode Mode { get; init; } = AgentMode.Plan;

      /// <summary>True when <see cref="ValidationReport"/> is empty.</summary>
    public bool IsValid => ValidationReport.Count == 0;
}