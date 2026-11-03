using DtPipe.Coordinator.Server.Access;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Abstractions;
using TransportR.Client;
using TransportR.Client.Services;
using TransportR.Interfaces;
using TransportR.Serialization.MessagePack;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// TransportR's clients against the coordinator's own request pipeline on Kestrel, not a hub assembled by a test: the
/// middleware that answers 401, the key issuer's scheme, the order the middlewares really run in, and the groups and the flow
/// matrix read from the rights. A client holds the token it got from the issuer with its key, and a transfer the coordinator
/// opens between two of them carries its items.
/// </summary>
public sealed class CoordinatorTransferTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Two_Clients_Holding_Keys_Exchange_Items_Through_The_Coordinators_Pipeline()
    {
        var port = KestrelCoordinator.FreePort();
        var url = $"http://127.0.0.1:{port}";
        var state = Path.Combine(Path.GetTempPath(), "coordinator-transfer-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(state);
        try
        {
            using var coordinator = new KestrelCoordinator(url, state);
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

            var sendHandle = new TaskCompletionSource<Send<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
            receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

            await sender.ConnectAsync();
            await receiver.ConnectAsync();
            var initiator = coordinator.Services.GetRequiredService<ITransferInitiator>();
            await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000);

            await using var send = await sendHandle.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
            await using var receive = await receiveHandle.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);

            var expected = Enumerable.Range(0, 100).Select(i => $"item-{i}").ToList();
            var consumer = Task.Run(async () =>
            {
                var items = new List<string>();
                await foreach (var item in receive.ReceiveAsync()) items.Add(item);
                await receive.WaitForCompletionAsync();
                return items;
            }, Ct);

            await send.SendAsync(expected);
            await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30), Ct);
            Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30), Ct));
        }
        finally
        {
            if (Directory.Exists(state)) Directory.Delete(state, recursive: true);
        }
    }

    private static TransportRClient<string> Client(string url, OidcTokenClient tokens) => new TransportRClientBuilder<string>()
        .WithUrl(url)
        .WithMessagePackSerialization()
        .WithTokenProvider(() => tokens.GetAsync())
        .Build();
}
