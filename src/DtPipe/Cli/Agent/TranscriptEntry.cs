using System.Collections.Generic;

namespace DtPipe.Cli.Agent;

/// <summary>What produced a transcript entry — the axis a full-screen surface styles and filters on.</summary>
internal enum TranscriptEntryKind
{
    /// <summary>A finished step: the one-line trace plus the model's stated-intent digest.</summary>
    Digest,

    /// <summary>The result of one tool call.</summary>
    ToolResult,

    /// <summary>The model's final answer for the turn.</summary>
    AgentResponse,

    /// <summary>A dump of the model's chain of thought (detail = full).</summary>
    Thinking,
}

/// <summary>
/// One unit of the agent transcript as UI-agnostic data. The scrollback path renders
/// <see cref="MarkupLines"/> verbatim (so the permanent record is byte-identical to before); a
/// full-screen surface renders <see cref="Text"/> and styles by <see cref="Kind"/>. Produced by
/// <see cref="StepDigest"/>; the markup lines are the single source the pre-existing
/// <c>StepDigest.Lines</c> / <c>AgentResponseLines</c> projections now read from.
/// </summary>
internal sealed record TranscriptEntry(
    TranscriptEntryKind Kind,
    string Text,
    IReadOnlyList<string> MarkupLines,
    bool IsError = false);
