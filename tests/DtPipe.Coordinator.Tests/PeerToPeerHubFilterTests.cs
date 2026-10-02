using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Coordinator.Tests.Infrastructure;
using TransportR.Abstractions;
using TransportR.FlowControl;
using TransportR.Interfaces;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// A peer calling <c>GetReceivers</c> or <c>InitTransfer</c> directly is refused; the coordinator's
/// own path to open a transfer, <see cref="ITransferInitiator"/>, is untouched by the filter because
/// it never invokes a hub method.
/// </summary>
public class PeerToPeerHubFilterTests
{
    [Fact]
    public async Task GetReceivers_CalledByAPeer_IsRefused()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var client = host.CreateClient();
        await client.ConnectAsync();

        var ex = await Assert.ThrowsAsync<HubException>(
            () => client.ControlConnection.InvokeAsync<List<TransportR.Models.ClientInfo>>("GetReceivers"));
        Assert.Contains("reserved to the coordinator", ex.Message);
    }

    [Fact]
    public async Task InitTransfer_CalledByAPeer_IsRefused()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var sender = host.CreateClient();
        await using var receiver = host.CreateClient();
        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var ex = await Assert.ThrowsAsync<HubException>(
            () => sender.ControlConnection.InvokeAsync<string>("InitTransfer", receiver.ClientId, 10, 5_000));
        Assert.Contains("reserved to the coordinator", ex.Message);
    }

    [Fact]
    public async Task TransferOpenedByTheCoordinator_PassesDespiteTheFilter()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });

        await using var sender = host.CreateClient();
        await using var receiver = host.CreateClient();

        var sendHandle = new TaskCompletionSource<Send<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveHandle = new TaskCompletionSource<Receive<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        sender.OnSendRequested += send => { sendHandle.TrySetResult(send); return Task.CompletedTask; };
        receiver.OnTransferStarted += receive => { receiveHandle.TrySetResult(receive); return Task.CompletedTask; };

        await sender.ConnectAsync();
        await receiver.ConnectAsync();

        var initiator = host.Host.Services.GetRequiredService<ITransferInitiator>();
        await initiator.InitTransferAsync(sender.ClientId, receiver.ClientId, batchSize: 10, timeoutMs: 20_000);

        await using var send = await sendHandle.Task;
        await using var receive = await receiveHandle.Task;

        var expected = Enumerable.Range(0, 100).Select(i => $"item-{i}").ToList();
        var consumer = Task.Run(async () =>
        {
            var items = new List<string>();
            await foreach (var item in receive.ReceiveAsync()) items.Add(item);
            await receive.WaitForCompletionAsync();
            return items;
        });

        await send.SendAsync(expected);
        await send.CompleteAsync().WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(expected, await consumer.WaitAsync(TimeSpan.FromSeconds(30)));
    }
}
