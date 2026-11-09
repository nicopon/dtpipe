using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class TransportRBufferOptionTests
{
    private static DtPipeNodeOptions BoundFrom(params (string Key, string Value)[] extra)
    {
        var settings = new Dictionary<string, string?>
        {
            ["DtPipeNode:Name"] = "node-1",
            ["DtPipeNode:CoordinatorUrl"] = "http://coordinator",
            ["DtPipeNode:Oidc:Authority"] = "http://idp",
            ["DtPipeNode:Oidc:ClientId"] = "node-1",
            ["DtPipeNode:Oidc:ClientSecret"] = "secret",
        };
        foreach (var (key, value) in extra) settings["DtPipeNode:" + key] = value;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var services = new ServiceCollection();
        services.AddDtPipeNode(configuration.GetSection("DtPipeNode"));
        return services.BuildServiceProvider().GetRequiredService<IOptions<DtPipeNodeOptions>>().Value;
    }

    [Fact]
    public void Defaults_are_256kb_batch_16_inflight_and_256kb_read_chunk()
    {
        var options = BoundFrom();
        Assert.Equal(262144, options.BatchBytes);
        Assert.Equal(16, options.MaxInFlightBatches);
        Assert.Equal(262144, options.ReadChunkBytes);
    }

    [Fact]
    public void Buffer_options_bind_from_configuration()
    {
        var options = BoundFrom(
            ("BatchBytes", "524288"),
            ("MaxInFlightBatches", "32"),
            ("ReadChunkBytes", "131072"));

        Assert.Equal(524288, options.BatchBytes);
        Assert.Equal(32, options.MaxInFlightBatches);
        Assert.Equal(131072, options.ReadChunkBytes);
    }

    [Fact]
    public async Task Non_positive_buffer_values_fail_validation()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDtPipeNode(o =>
        {
            o.Name = "node-1";
            o.CoordinatorUrl = "http://coordinator";
            o.Oidc.ClientSecret = "secret";
            o.BatchBytes = 0;
            o.MaxInFlightBatches = -1;
            o.ReadChunkBytes = -100;
        });
        using var host = builder.Build();

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("DtPipeNode: BatchBytes must be positive.", error.Failures);
        Assert.Contains("DtPipeNode: MaxInFlightBatches must be positive.", error.Failures);
        Assert.Contains("DtPipeNode: ReadChunkBytes must be positive.", error.Failures);
    }
}

