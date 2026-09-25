using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Coordinator.Tests.Infrastructure;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// The rendez-vous barrier: a run starts only once every fragment it names has registered.
/// <see cref="AdmissionGate"/> is exercised here against the real hub and <see cref="NodeRegistry"/>
/// singleton a <see cref="CoordinatorHub.Register"/> call populates, not a fake.
/// </summary>
public class AdmissionGateTests
{
    [Fact]
    public async Task AllFragmentsRegistered_IsAdmitted()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a");
        await b.ControlConnection.InvokeAsync("Register", "fragment-b");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var admitted = await gate.AwaitAllAsync(["fragment-a", "fragment-b"], TimeSpan.FromSeconds(5));

        Assert.Equal(["fragment-a", "fragment-b"], admitted.Select(n => n.FragmentName).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task AnAbsentFragment_RefusesTheRun_NamingIt()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await a.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var ex = await Assert.ThrowsAsync<AdmissionRefusedException>(
            () => gate.AwaitAllAsync(["fragment-a", "fragment-b"], TimeSpan.FromMilliseconds(200)));

        Assert.Contains("fragment-b", ex.AbsentFragments);
        Assert.DoesNotContain("fragment-a", ex.AbsentFragments);
    }

    [Fact]
    public async Task ARegistrationArrivingLate_StillAdmitsBeforeTheTimeout()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });

        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();
        await a.ControlConnection.InvokeAsync("Register", "fragment-a");

        var gate = host.Host.Services.GetRequiredService<AdmissionGate>();
        var admission = gate.AwaitAllAsync(["fragment-a", "fragment-b"], TimeSpan.FromSeconds(5));

        await Task.Delay(100);
        await b.ControlConnection.InvokeAsync("Register", "fragment-b");

        var admitted = await admission.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2, admitted.Count);
    }
}
