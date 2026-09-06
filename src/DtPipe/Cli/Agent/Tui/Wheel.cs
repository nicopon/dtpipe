namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// The braille wheel that says something is still working, and the tick that turns it. One
/// definition, so the caret and the running step in the list turn in step with each other rather
/// than at two phases of the same animation.
/// </summary>
internal static class Wheel
{
    private static readonly string[] Frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

    /// <summary>The frame for a monotonically rising tick.</summary>
    public static string Frame(int tick) => Frames[tick % Frames.Length];
}
