namespace DtPipe.Cli.Incremental;

/// <summary>
/// The value every <c>${{cursor://…}}</c> resolves to when one is given, ahead of the state file and the default. Two
/// sources, one answer: <c>--cursor-from</c>, armed for the run it is given to and flowing with it like
/// <see cref="DtPipe.Cli.Expressions.LaunchValues"/>, and <see cref="Variable"/>, which a pipeline node gives the
/// <c>dtpipe</c> it starts, read once for the process. The flag wins. Nothing writes the process environment: a value
/// written there would reach every branch, every run beside it and every child process.
/// </summary>
public static class CursorOverride
{
    /// <summary>The variable a node sets for its child; <c>FragmentCursors.OverrideVariable</c> names the same.</summary>
    public const string Variable = "DTPIPE_CURSOR_OVERRIDE";

    private static readonly string? FromProcess = NonBlank(Environment.GetEnvironmentVariable(Variable));

    private static readonly AsyncLocal<string?> Armed = new();

    /// <summary>Gives <paramref name="value"/> to every cursor resolved inside the returned scope; null gives none.</summary>
    public static IDisposable Arm(string? value)
    {
        var previous = Armed.Value;
        Armed.Value = NonBlank(value);
        return new Restorer(previous);
    }

    internal static string? Value => Armed.Value ?? FromProcess;

    private static string? NonBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    private sealed class Restorer(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Armed.Value = previous;
        }
    }
}
