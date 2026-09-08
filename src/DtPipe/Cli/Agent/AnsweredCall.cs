using System;
using System.Text.Json;

namespace DtPipe.Cli.Agent;

/// <summary>
/// What a tool call answered earlier in this turn, kept so an identical call that comes back with
/// an identical answer can be named as the repeat it is.
///
/// <para>
/// The role prompt states the rule — repeating a call unchanged spends a turn for nothing — and
/// nothing applied it. A recorded session emitted the same 2 250-character <c>validate-yaml-job</c>
/// call twice in a row, with no reasoning and no thinking between them, and was handed the same
/// error back as if it were news.
/// </para>
///
/// <para>
/// The repeat is established on the <em>pair</em>, and the call is dispatched every time. Deciding
/// on the question alone would mean answering from a cache and telling the model the call "cannot
/// return anything new" — a claim about a source another process is free to change, and about a
/// pipeline whose own sampling is random without a seed. Comparing the answer that actually came
/// back states only what was observed.
/// </para>
///
/// <para>
/// The advice wraps the answer rather than replacing it, so the model keeps everything it had plus
/// the one fact it was missing, and the call's own error flag is untouched: a repeat of a failing
/// call still reads as a failure to correct.
/// </para>
/// </summary>
/// <param name="Step">1-based position of the step this answer was first seen at.</param>
/// <param name="Answer">The tool result, as the model received it.</param>
internal sealed record AnsweredCall(int Step, string Answer)
{
    /// <summary>
    /// Identity of a call: its name plus the raw text of its arguments. Two spellings of the same
    /// arguments read as different pairs and simply go unnoticed, which is the harmless direction —
    /// this may miss a repeat, it must never invent one.
    /// </summary>
    internal static string KeyOf(ToolCall call) =>
        call.Name + " " + (call.Arguments.ValueKind == JsonValueKind.Undefined
            ? "{}"
            : call.Arguments.GetRawText());

    internal string Advise(string answer) =>
        $"[repeated call] You already made this exact call at step {Step}, and it has just returned "
      + "the same answer again, reproduced below. Calling it a third time will not change it: "
      + "change the arguments, or act on what it says.\n\n"
      + answer;
}
