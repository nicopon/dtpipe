using System;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The verdict a finished turn carries. Three states, not two: a turn that stopped to ask the user
/// something neither succeeded nor failed, and reporting it as either would be a lie the rest of
/// the agent has to work around.
/// </summary>
public enum TurnStatus
{
    /// <summary>The model delivered a plan or an answer. The only state that exits 0.</summary>
    Completed,

    /// <summary>The model asked a question and is waiting for the reply.</summary>
    AwaitingInput,

    /// <summary>The turn stopped without a result — an error, an interrupt, an exhausted budget.</summary>
    Incomplete,
}

/// <summary>
/// What one finished turn amounted to, as data. Built once by the executor and rendered twice: as
/// the scrollback table (<see cref="AgentTui.RenderFinalSummary"/>) and as the full-screen
/// surface's exchange (<see cref="Tui.ExchangeContent.Of"/>). One factory, two projections — so the
/// two surfaces can never disagree about whether a turn succeeded, and the verdict has a single
/// author.
/// </summary>
/// <param name="Outcome">Why the turn ended; everything else here is derived from it.</param>
/// <param name="Iterations">Model calls the turn consumed, clamped to the budget.</param>
/// <param name="Duration">Wall time from the prompt to the verdict.</param>
/// <param name="ToolCounts">Tool name to call count, in first-call order.</param>
/// <param name="Question">The question the model asked and the choices it offered, when
/// <paramref name="Outcome"/> is <see cref="TurnOutcome.AwaitingUserInput"/>.</param>
/// <param name="ProducedPlan">True when <em>this</em> turn produced plan YAML. Drives the
/// plan-mode "next steps" line, which speaks to the plan this turn made — not one an earlier turn
/// left on the trajectory.</param>
/// <param name="ClosingWords">What the model said to finish the turn, when it finished with words
/// rather than a tool call. The exchange band shows it: a turn that ended on a sentence used to
/// leave a tally in its place, so the sentence — a question among them — was only reachable by
/// selecting the last step.</param>
/// <param name="Tokens">Completion tokens the turn consumed, as the provider reported them. Zero
/// when no provider reported any. The scrollback table does not print it — its rows are a fixed
/// contract — so this reaches the reader through the full-screen exchange only.</param>
public sealed record TurnSummaryModel(
    TurnOutcome Outcome,
    int Iterations,
    TimeSpan Duration,
    IReadOnlyDictionary<string, int> ToolCounts,
    AgentQuestion? Question = null,
    bool ProducedPlan = false,
    long Tokens = 0,
    string? ClosingWords = null)
{
    public TurnStatus Status => Outcome switch
    {
        TurnOutcome.Succeeded => TurnStatus.Completed,
        TurnOutcome.AwaitingUserInput => TurnStatus.AwaitingInput,
        _ => TurnStatus.Incomplete,
    };

    /// <summary>The process exit code this turn earns. Only a completed turn is a success.</summary>
    public int ExitCode => Status == TurnStatus.Completed ? 0 : 1;

    /// <summary>The verdict as words, without styling — each surface adds its own.</summary>
    public string StatusWord => Status switch
    {
        TurnStatus.Completed => "🟢 COMPLETED",
        TurnStatus.AwaitingInput => "❓ AWAITING INPUT",
        _ => "❌ INCOMPLETE",
    };

    public string Reason => Describe(Outcome);

    public string DurationText => $"{Duration.TotalSeconds:F2} seconds";

    /// <summary>How many tool calls the turn made, across every tool.</summary>
    public int TotalToolCalls => ToolCounts.Values.Sum();

    /// <summary>The tool tally as one line, or null when no tool ran.</summary>
    public string? ToolSummary => ToolCounts.Count == 0
        ? null
        : string.Join(", ", ToolCounts.Select(kv => $"{kv.Key}: {kv.Value}"));

    /// <summary>Why the turn ended, in a clause that completes "the turn ended because it …".</summary>
    public static string Describe(TurnOutcome outcome) => outcome switch
    {
        TurnOutcome.Succeeded => "delivered a response",
        TurnOutcome.AwaitingUserInput => "the agent asked you a question",
        TurnOutcome.UserInterrupted => "you interrupted the model call",
        TurnOutcome.MaxIterationsReached => "hit the iteration limit without finishing",
        TurnOutcome.LlmError => "the LLM call failed",
        TurnOutcome.EmptyResponse => "the model returned an empty response",
        TurnOutcome.RepetitionDetected => "the model got stuck repeating itself",
        _ => outcome.ToString(),
    };
}
