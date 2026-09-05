namespace DtPipe.Cli.Agent;

/// <summary>
/// Why an agent turn ended. Surfaced to the user so a turn never stops without a stated reason,
/// and so the session summary's verdict matches what actually happened.
///
/// Only <see cref="Succeeded"/> maps to exit code 0. The rest are the ways a turn stops without a
/// finished result; they were previously indistinguishable at the summary, and
/// <see cref="EmptyResponse"/> in particular was reported as a success.
///
/// <see cref="AwaitingUserInput"/> is the odd one out: not a failure, just unfinished. It reports
/// a non-zero exit like the failures — an agent that could not finish for want of an answer did
/// not succeed — but interactively it is a pause, not a stop: the user answers and the session
/// continues.
/// </summary>
public enum TurnOutcome
{
    /// <summary>The model delivered a substantive answer — a validated plan, or a direct reply.</summary>
    Succeeded,

    /// <summary>The model called <c>ask-user</c>: it needs a decision only the user can make and
    /// stopped to get it. The user's next message is the answer.</summary>
    AwaitingUserInput,

    /// <summary>The loop ran out of iterations before the model delivered a plan or an answer.</summary>
    MaxIterationsReached,

    /// <summary>The LLM call failed — endpoint unreachable, timed out, or a provider error.</summary>
    LlmError,

    /// <summary>The model stopped with an empty response and no tool call — nothing was produced.</summary>
    EmptyResponse,

    /// <summary>The model was seen regenerating the same text and the call was stopped early
    /// (<see cref="RepetitionGuard"/>) — a decoding pathology, not an endpoint problem.</summary>
    RepetitionDetected,

    /// <summary>
    /// The user pressed Esc during a model call on the full-screen surface. The call was
    /// abandoned, whatever the turn had already done is kept, and the session stays open — so
    /// this is a soft stop, never the process-wide cancellation that exits 130.
    /// </summary>
    UserInterrupted,
}
