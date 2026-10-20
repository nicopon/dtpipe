using System.Collections.Concurrent;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A host whose coordinator does not answer tries again at a growing interval, not once a second: in the first five seconds it
/// tries at 0, 1 and 3 s, where a fixed second would have tried six times.
/// </summary>
public class ConnectBackoffTests : IDisposable
{
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("agent-backoff-");

    public void Dispose() => _dir.Delete(recursive: true);

    private sealed class Attempts : ILoggerProvider
    {
        public ConcurrentQueue<string> Retries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Counter(Retries);

        public void Dispose() { }

        private sealed class Counter(ConcurrentQueue<string> retries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                var line = formatter(state, exception);
                if (line.Contains("retrying")) retries.Enqueue(line);
            }
        }
    }

    [Fact]
    public async Task AnUnreachableCoordinator_IsTriedAgainAtAGrowingInterval()
    {
        var attempts = new Attempts();
        var options = new DtPipeNodeOptions
        {
            Name = "node-x",
            CoordinatorUrl = "http://127.0.0.1:1",
            DtPipe = Path.Combine(_dir.FullName, "no-such-dtpipe"),
            StateDirectory = Path.Combine(_dir.FullName, "state"),
            Oidc = new NodeOidcOptions { Authority = "http://127.0.0.1:1", ClientId = "node-x", ClientSecret = "secret" },
        };
        using var agent = new NodeAgent(Options.Create(options), new NodeDefinition(), LoggerFactory.Create(b => b.AddProvider(attempts)));
        await agent.StartAsync(CancellationToken.None);

        await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await agent.StopAsync(CancellationToken.None);

        Assert.InRange(attempts.Retries.Count, 2, 4);
    }
}
