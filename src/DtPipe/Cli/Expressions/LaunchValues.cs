using DtPipe.Core.Expressions;

namespace DtPipe.Cli.Expressions;

/// <summary>
/// The values a launcher gives the variables of the one action it runs, ambient like
/// <see cref="NonInteractiveGuard"/>: they flow with the logical call, through <c>await</c> and into the
/// tasks that run a DAG's branches, and no other call sees them, whatever runs beside it. Nothing is
/// written to the process environment, so a concurrent request cannot read a value it was not given.
/// </summary>
public static class LaunchValues
{
    private static readonly AsyncLocal<IReadOnlyDictionary<string, string>?> Armed = new();

    /// <summary>Gives <paramref name="values"/> to every variable resolved inside the returned scope.</summary>
    public static IDisposable Arm(IReadOnlyDictionary<string, string> values)
    {
        var previous = Armed.Value;
        Armed.Value = values;
        return new Restorer(previous);
    }

    internal static string? Get(string name) => Armed.Value is { } values && values.TryGetValue(name, out var value) ? value : null;

    private sealed class Restorer(IReadOnlyDictionary<string, string>? previous) : IDisposable
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

/// <summary>
/// Resolves a <c>${{NAME}}</c> to the value its launcher armed, and to nothing otherwise. Registered ahead of
/// <see cref="EnvVarInterpolator"/>: the value a person chose wins over the environment, as a run's own would.
/// </summary>
public sealed class LaunchValueInterpolator : IStringInterpolator
{
    public Task<string?> TryResolveAsync(string expression, CancellationToken ct) => Task.FromResult(LaunchValues.Get(expression));
}
