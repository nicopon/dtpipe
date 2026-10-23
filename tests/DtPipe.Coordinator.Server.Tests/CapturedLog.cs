using Microsoft.Extensions.Logging;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>What every logger of the host wrote, as text.</summary>
internal sealed class CapturedLog : ILoggerProvider
{
    private readonly List<string> _entries = [];

    public IReadOnlyList<string> Entries
    {
        get { lock (_entries) return [.. _entries]; }
    }

    public ILogger CreateLogger(string categoryName) => new Capture(this);

    public void Dispose() { }

    private sealed class Capture(CapturedLog owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (owner._entries) owner._entries.Add(formatter(state, exception) + exception);
        }
    }
}
