using System.Threading.Channels;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// Queues log lines for the coordinator's event stream. Bounded and lossy on purpose: a slow or
/// absent coordinator must never block a relay thread that happens to log.
/// </summary>
internal sealed class NodeLogForwarder
{
    private readonly Channel<NodeLogLine> _lines = Channel.CreateBounded<NodeLogLine>(
        new BoundedChannelOptions(2048) { FullMode = BoundedChannelFullMode.DropOldest });

    public ChannelReader<NodeLogLine> Reader => _lines.Reader;

    /// <summary>
    /// A logger factory that writes to the host's own loggers and to the coordinator, tagged with
    /// <paramref name="fragment"/> and the <paramref name="runId"/> it runs in (both null for a line of the node itself).
    /// The child's stderr is logged at Debug, so the coordinator gets Debug and up; TransportR and the framework only from
    /// Warning. Disposing it leaves the host's factory alone.
    /// </summary>
    public ILoggerFactory For(ILoggerFactory host, string node, string? fragment, string? runId) => new Factory(host, this, node, fragment, runId);

    private sealed class Factory(ILoggerFactory host, NodeLogForwarder sink, string node, string? fragment, string? runId) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => new Tee(host.CreateLogger(categoryName), sink, node, fragment, runId, categoryName,
            LogCategories.IsTransport(categoryName) || categoryName.StartsWith("Microsoft", StringComparison.Ordinal)
                ? LogLevel.Warning
                : LogLevel.Debug);

        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class Tee(ILogger host, NodeLogForwarder sink, string node, string? fragment, string? runId, string category, LogLevel forwarded) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => host.BeginScope(state);
        public bool IsEnabled(LogLevel logLevel) => logLevel >= forwarded || host.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            host.Log(logLevel, eventId, state, exception, formatter);
            if (logLevel < forwarded || logLevel == LogLevel.None) return;
            var message = formatter(state, exception);
            if (exception is not null) message += $" ({exception.GetType().Name}: {exception.Message})";
            sink._lines.Writer.TryWrite(new NodeLogLine(node, fragment, logLevel.ToString(), message, category, runId));
        }
    }
}
