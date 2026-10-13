using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>Whether a host lets the coordinator end a run's child is its own configuration's word: off unless the section says so.</summary>
public class FaultInjectionOptionTests
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
    public void A_host_whose_configuration_says_nothing_does_not_enable_fault_injection()
    {
        Assert.False(BoundFrom().FaultInjection);
    }

    [Fact]
    public void A_host_whose_configuration_enables_it_does()
    {
        Assert.True(BoundFrom(("FaultInjection", "true")).FaultInjection);
    }
}
