using System.Threading.Channels;
using DtPipe.Lab.Contracts;
using Microsoft.Extensions.Logging;

namespace DtPipe.Lab.NodeHost;

/// <summary>
/// Queues log lines for the coordinator's event stream. Bounded and lossy on purpose: a slow or
/// absent coordinator must never block a relay thread that happens to log.
/// </summary>
public sealed class LabLogForwarder
{
    private readonly Channel<NodeLogLine> _lines = Channel.CreateBounded<NodeLogLine>(
        new BoundedChannelOptions(2048) { FullMode = BoundedChannelFullMode.DropOldest });

    public ChannelReader<NodeLogLine> Reader => _lines.Reader;

    public void Post(NodeLogLine line) => _lines.Writer.TryWrite(line);

    public ILoggerProvider CreateProvider(string node, string? fragment) => new Provider(this, node, fragment);

    private sealed class Provider(LabLogForwarder sink, string node, string? fragment) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new Logger(sink, node, fragment);
        public void Dispose() { }
    }

    private sealed class Logger(LabLogForwarder sink, string node, string? fragment) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (exception is not null) message += $" ({exception.GetType().Name}: {exception.Message})";
            sink.Post(new NodeLogLine(node, fragment, logLevel.ToString(), message));
        }
    }
}
