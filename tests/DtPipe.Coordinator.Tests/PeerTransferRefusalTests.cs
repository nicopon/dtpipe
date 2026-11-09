using Microsoft.Extensions.DependencyInjection;
using DtPipe.Coordinator.Tests.Infrastructure;
using TransportR.Abstractions;
using TransportR.FlowControl;
using TransportR.Interfaces;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// A peer asking the hub for receivers or to open a transfer is refused, since <c>AddCoordinatorHub</c> sets
/// <c>AccessOptions.ClientsMayOpenTransfers</c> to <c>false</c>; the coordinator's own path to open a transfer,
/// <see cref="ITransferInitiator"/>, is not affected by it.
/// </summary>
public class PeerTransferRefusalTests
{
    [Fact]
    public async Task GetReceivers_AskedByAPeer_IsRefused()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var client = host.CreateClient();
        await client.ConnectAsync();

        var ex = await Assert.ThrowsAsync<HubRequestFailedException>(() => client.GetReceiversAsync());
        Assert.Contains("The hub refused the request.", ex.Message);
    }

    [Fact]
    public async Task InitTransfer_AskedByAPeer_IsRefused()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var sender = host.CreateClient();
        await using var receiver = host.CreateClient();
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var ex = await Assert.ThrowsAsync<HubRequestFailedException>(
            () => sender.InitTransferAsync(receiver.ClientId));
        Assert.Contains("The hub refused the request.", ex.Message);
    }

    [Fact]
    public async Task TransferOpenedByTheCoordinator_PassesDespiteTheRefusalOfPeers()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });

        await using var sender = host.CreateClient();
        await using var receiver = host.CreateClient();

        var sendHandle = new TaskCompletionSource<ByteSend>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveHandle = new TaskCompletionSource<ByteReceive>(TaskCreationOptions.RunContinuationsAsynchronously);
        sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var initiator = host.Host.Services.GetRequiredService<ITransferInitiator>();
        await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, timeoutMs: 20_000);

        await using var send = await sendHandle.Task;
        await using var receive = await receiveHandle.Task;

        var expected = new byte[] { 1, 2, 3, 4, 5 };
        var consumer = Task.Run(async () =>
        {
            var buffer = new byte[expected.Length];
            int totalRead = 0;
            while (totalRead < buffer.Length)
            {
                int read = await receive.ReadAsync(buffer.AsMemory(totalRead));
                if (read == 0) break;
                totalRead += read;
            }
            await receive.WaitForCompletionAsync();
            return buffer;
        });

        await send.WriteAsync(expected);
        await send.CompleteAsync();
        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }
}
