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
    /// <paramref name="fragment"/>. The child's stderr is logged at Debug, so the coordinator gets
    /// Debug and up; TransportR and the framework only from Warning. Disposing it leaves the host's
    /// factory alone.
    /// </summary>
    public ILoggerFactory For(ILoggerFactory host, string node, string? fragment) => new Factory(host, this, node, fragment);

    private sealed class Factory(ILoggerFactory host, NodeLogForwarder sink, string node, string? fragment) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => new Tee(host.CreateLogger(categoryName), sink, node, fragment,
            categoryName.StartsWith("TransportR", StringComparison.Ordinal) || categoryName.StartsWith("Microsoft", StringComparison.Ordinal)
                ? LogLevel.Warning
                : LogLevel.Debug);

        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class Tee(ILogger host, NodeLogForwarder sink, string node, string? fragment, LogLevel forwarded) : ILogger
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
            sink._lines.Writer.TryWrite(new NodeLogLine(node, fragment, logLevel.ToString(), message));
        }
    }
}
