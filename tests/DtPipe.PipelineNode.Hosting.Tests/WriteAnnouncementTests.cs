using System.Collections.Concurrent;
using System.Threading.Channels;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server;
using DtPipe.Coordinator.Server.Access;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TransportR.Client;
using TransportR.Hub;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// What a real transfer and a real probe make the coordinator say: the clients are TransportR's, with the
/// probe the node enables, and the hub is the coordinator's own pipeline on Kestrel. Each announcement the hub
/// makes reaches the coordinator's log and the pages' log (the event bus the pages read).
/// </summary>
public sealed class WriteAnnouncementTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Lines : ILoggerProvider
    {
        private readonly ConcurrentQueue<string> _entries = new();
        public IReadOnlyCollection<string> Entries => _entries;
        public ILogger CreateLogger(string categoryName) => new Capture(categoryName, _entries);
        public void Dispose() { }

        private sealed class Capture(string category, ConcurrentQueue<string> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                entries.Enqueue($"{logLevel} {category}: {formatter(state, exception)}");
        }
    }

    /// <summary>The next line of the pages' log that satisfies <paramref name="match"/>, from what the bus kept and what it publishes.</summary>
    private static async Task<string> PagesLine(EventBus bus, Func<string, bool> match)
    {
        var (backlog, live, unsubscribe) = bus.Subscribe();
        try
        {
            foreach (var evt in backlog)
                if (evt is { Type: "log", Data: NodeLogLine { Message: var m } } && match(m)) return m;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            await foreach (var evt in live.ReadAllAsync(timeout.Token))
                if (evt is { Type: "log", Data: NodeLogLine { Message: var m } } && match(m)) return m;
            throw new TimeoutException("the pages' log never showed the line");
        }
        finally { unsubscribe(); }
    }

    [Fact]
    public async Task A_Probe_And_A_Transfer_Are_Announced_In_The_Coordinators_Log_And_The_Pages_Log()
    {
        var port = KestrelCoordinator.FreePort();
        var url = $"http://127.0.0.1:{port}";
        var state = Path.Combine(Path.GetTempPath(), "coordinator-announce-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(state);
        try
        {
            var lines = new Lines();
            using var coordinator = new KestrelCoordinator(url, state) { LogTo = lines };
            coordinator.UseKestrel(port);
            coordinator.StartServer();

            var rights = coordinator.Services.GetRequiredService<RightsStore>();
            rights.AddIdentity(new Identity("source", "source", "", ["upstream"]) { Auth = new IdentityAuth(AuthModes.Key, []) });
            rights.AddIdentity(new Identity("sink", "sink", "", ["downstream"]) { Auth = new IdentityAuth(AuthModes.Key, []) });
            rights.AllowEdges([new GroupEdge("upstream", "downstream")]);
            var (sourceKey, _) = rights.AddKey("source", null, null);
            var (sinkKey, _) = rights.AddKey("sink", null, null);
            using var sourceTokens = new OidcTokenClient(new NodeOidcOptions { Authority = url, ClientSecret = sourceKey.Reveal() }, "source", url);
            using var sinkTokens = new OidcTokenClient(new NodeOidcOptions { Authority = url, ClientSecret = sinkKey.Reveal() }, "sink", url);

            await using var sender = Client(url, sourceTokens);
            await using var receiver = Client(url, sinkTokens);
            var sendHandle = new TaskCompletionSource<ByteSend>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
            sender.OnSendRequested(send => { sendHandle.TrySetResult(send); return Task.CompletedTask; });
            receiver.OnTransferStarted(receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; });
            await sender.ConnectAsync();
            await receiver.ConnectAsync();

            // The client probes in the background after it registers; the hub's announcement of it is a line of each log.
            var bus = coordinator.Services.GetRequiredService<EventBus>();
            var probed = await PagesLine(bus, m => m.Contains(sender.ClientId.ToString()) && m.Contains("probed its write path"));
            Assert.Contains("WebSocket", probed);

            var initiator = coordinator.Services.GetRequiredService<ITransferInitiator>();
            var transferId = await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, handshakeTimeout: TimeSpan.FromSeconds(20));
            await using var send = await sendHandle.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
            await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
            var expected = new byte[10_000];
            Random.Shared.NextBytes(expected);
            var consumer = Task.Run(async () =>
            {
                using var ms = new MemoryStream();
                var buffer = new byte[4096];
                while (true)
                {
                    var read = await receive.ReadAsync(buffer, Ct);
                    if (read == 0) break;
                    ms.Write(buffer, 0, read);
                }
                await receive.WaitForCompletionAsync(Ct);
                return ms.ToArray();
            }, Ct);
            await send.WriteAsync(expected, Ct);
            await send.CompleteAsync(Ct).WaitAsync(TimeSpan.FromSeconds(30), Ct);
            Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30), Ct));

            var opened = await PagesLine(bus, m => m.Contains(transferId) && m.Contains(sender.ClientId.ToString()) && m.Contains("writes its"));
            Assert.Contains("WebSocket", opened);

            var logged = lines.Entries.Where(e => e.StartsWith("Information DtPipe.Coordinator.LoggingHubProgressMonitor:", StringComparison.Ordinal)).ToList();
            Assert.Contains(logged, e => e.Contains(sender.ClientId.ToString()) && e.Contains("probed its write path") && e.Contains("WebSocket"));
            Assert.Contains(logged, e => e.Contains(transferId) && e.Contains(sender.ClientId.ToString()) && e.Contains("writes its") && e.Contains("WebSocket"));
        }
        finally
        {
            if (Directory.Exists(state)) Directory.Delete(state, recursive: true);
        }
    }

    private static ByteStreamClient Client(string url, OidcTokenClient tokens) => new ByteStreamClientBuilder(batchBytes: 65536)
        .WithUrl(url)
        .WithTokenProvider(() => tokens.GetAsync())
        .WithWriteTransportProbe()
        .Build();
}
