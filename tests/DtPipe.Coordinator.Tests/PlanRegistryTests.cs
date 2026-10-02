using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using TransportR.FlowControl;
using Xunit;

namespace DtPipe.Coordinator.Tests;

public class PlanRegistryTests
{
    private static PlanRegistry CreateRegistry(Action<FlowControlOptions> configure)
    {
        var options = new FlowControlOptions();
        configure(options);
        return new PlanRegistry(new DefaultFlowControlService(options));
    }

    [Fact]
    public void Register_AllEdgesPermitted_Succeeds()
    {
        var registry = CreateRegistry(o => o.Groups["orders-producer"] = new GroupAccess
        {
            CanSendTo = ["orders-consumer"]
        });

        var plan = new FlowPlan("join", [new PlanEdge("orders", "orders-producer", "orders-consumer")]);

        registry.Register(plan);
    }

    [Fact]
    public void Register_OneForbiddenEdge_ThrowsAndNamesIt()
    {
        var registry = CreateRegistry(o => o.Groups["orders-producer"] = new GroupAccess
        {
            CanSendTo = ["orders-consumer"]
        });

        var plan = new FlowPlan("join",
        [
            new PlanEdge("orders", "orders-producer", "orders-consumer"),
            new PlanEdge("customers", "customers-producer", "orders-consumer"),
        ]);

        var ex = Assert.Throws<PlanRejectedException>(() => registry.Register(plan));

        Assert.Single(ex.ForbiddenEdges);
        Assert.Equal("customers", ex.ForbiddenEdges[0].Name);
        Assert.Contains("customers", ex.Message);
        Assert.Contains("customers-producer", ex.Message);
        Assert.Contains("orders-consumer", ex.Message);
    }

    [Fact]
    public async Task Register_ResolvedFromTheHub_UsesTheSameMatrixAsRuntimeTransfers()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["a"] = new GroupAccess { CanSendTo = ["b"] });

        var registry = host.Host.Services.GetRequiredService<IPlanRegistry>();

        registry.Register(new FlowPlan("a-to-b", [new PlanEdge("edge", "a", "b")]));

        var ex = Assert.Throws<PlanRejectedException>(() =>
            registry.Register(new FlowPlan("b-to-a", [new PlanEdge("edge", "b", "a")])));
        Assert.Equal("edge", ex.ForbiddenEdges[0].Name);
    }
}
