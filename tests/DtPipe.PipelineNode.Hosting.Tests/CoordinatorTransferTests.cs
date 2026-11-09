using DtPipe.Coordinator.Server.Access;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Abstractions;
using TransportR.Client;
using TransportR.Client.Services;
using TransportR.Interfaces;
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

            var sendHandle = new TaskCompletionSource<ByteSend>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
            sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
            receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

            await sender.ConnectAsync();
            await receiver.ConnectAsync();
            var initiator = coordinator.Services.GetRequiredService<ITransferInitiator>();
            await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, timeoutMs: 20_000);

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
        }
        finally
        {
            if (Directory.Exists(state)) Directory.Delete(state, recursive: true);
        }
    }

    private static ByteStreamClient Client(string url, OidcTokenClient tokens) => new ByteStreamClientBuilder(batchBytes: 65536)
        .WithUrl(url)
        .WithTokenProvider(() => tokens.GetAsync())
        .Build();
}
