using DtPipe.Cli.Commands;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class McpCommandTests
{
    private sealed class RecordingService : IHostedService
    {
        public int Started;
        public int Stopped;
        public Task StartAsync(CancellationToken cancellationToken) { Started++; return Task.CompletedTask; }
        public Task StopAsync(CancellationToken cancellationToken) { Stopped++; return Task.CompletedTask; }
    }

    [Fact]
    public async Task A_Cancellation_Exits_130_And_Stops_The_Services()
    {
        var service = new RecordingService();
        var provider = new ServiceCollection().AddSingleton<IHostedService>(service).BuildServiceProvider();
        using var cts = new CancellationTokenSource();

        var run = McpCommand.RunAsync(provider, cts.Token);
        await Task.Delay(50);
        cts.Cancel();

        Assert.Equal(130, await run);
        Assert.Equal(1, service.Started);
        Assert.Equal(1, service.Stopped);
    }

    [Fact]
    public async Task A_Cancellation_During_Start_Exits_130_Too()
    {
        var provider = new ServiceCollection().AddSingleton<IHostedService>(new RecordingService()).BuildServiceProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Equal(130, await McpCommand.RunAsync(provider, cts.Token));
    }
}
