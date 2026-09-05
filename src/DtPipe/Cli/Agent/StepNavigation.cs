using System.Collections.Generic;

namespace DtPipe.Cli.Agent;

/// <summary>
/// The bits of trajectory navigation shared between <see cref="SessionReview"/> (scrollback) and the
/// full-screen steps panel — so "jump to the next error" means the same thing on both surfaces.
/// </summary>
internal static class StepNavigation
{
    /// <summary>
    /// The index of the next step from <paramref name="from"/> in <paramref name="direction"/>
    /// (<c>+1</c> / <c>-1</c>) whose <see cref="TrajectoryStep.IsError"/> is set, or
    /// <paramref name="from"/> unchanged when there is none.
    /// </summary>
    public static int NextError(IReadOnlyList<TrajectoryStep> steps, int from, int direction)
    {
        for (int i = from + direction; i >= 0 && i < steps.Count; i += direction)
            if (steps[i].IsError)
                return i;
        return from;
    }
}
