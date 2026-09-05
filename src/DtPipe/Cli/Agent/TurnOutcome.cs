namespace DtPipe.Cli.Agent;

/// <summary>
/// Why an agent turn ended. Surfaced to the user so a turn never stops without a stated reason,
/// and so the session summary's verdict matches what actually happened.
///
/// Only <see cref="Succeeded"/> maps to exit code 0. The other three are the ways a turn stops
/// with nothing to show; they were previously indistinguishable at the summary, and
/// <see cref="EmptyResponse"/> in particular was reported as a success.
/// </summary>
public enum TurnOutcome
{
    /// <summary>The model delivered a substantive answer — a validated plan, or a direct reply.</summary>
    Succeeded,

    /// <summary>The loop ran out of iterations before the model delivered a plan or an answer.</summary>
    MaxIterationsReached,

    /// <summary>The LLM call failed — endpoint unreachable, timed out, or a provider error.</summary>
    LlmError,

    /// <summary>The model stopped with an empty response and no tool call — nothing was produced.</summary>
    EmptyResponse,

    /// <summary>The model was seen regenerating the same text and the call was stopped early
    /// (<see cref="RepetitionGuard"/>) — a decoding pathology, not an endpoint problem.</summary>
    RepetitionDetected,
}
