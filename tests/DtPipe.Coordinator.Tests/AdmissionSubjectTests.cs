using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Client.SignalR.Services;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// Admission binds a fragment to the identity of its node, against a hub that attests subjects: two
/// clients carry two tokens, each a fake node with no edge. A client of another subject that registers
/// the fragment's name is neither admitted nor counted.
/// </summary>
public class AdmissionSubjectTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static Task<CoordinatorTestHost> StartAsync(TestJwt jwt) =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(1),
            ReadyTimeout = TimeSpan.FromSeconds(2),
            ExitTimeout = TimeSpan.FromSeconds(1),
        }), jwt);

    private static string Token(TestJwt jwt, string subject) => jwt.Mint(subject, "test", TimeSpan.FromHours(1));

    /// <summary>A node that answers a <c>Launch</c> with <c>Ready</c> and a successful <c>Exited</c>, and notes it was launched.</summary>
    private static async Task<(SignalRDataClient<string> Client, TaskCompletionSource<string> Launched)> StartNodeAsync(
        CoordinatorTestHost host, string token, string fragment, string version)
    {
        var client = host.CreateClient(token);
        await client.ConnectAsync();
        var launched = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.ControlConnection.On<string>("Launch", async runId =>
        {
            launched.TrySetResult(runId);
            await client.ControlConnection.SendAsync("Ready", runId);
            await client.ControlConnection.SendAsync("Exited", runId, 0, "Local", (string?)null, new Dictionary<string, long>());
        });
        await client.ControlConnection.InvokeAsync("Register", fragment, version);
        return (client, launched);
    }

    [Fact]
    public async Task TheHubRecordsTheSubjectItAttested()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);

        var (node, _) = await StartNodeAsync(host, Token(jwt, "node-legit"), "frag-a", "v1");
        await using var _node = node;

        var instance = Assert.Single(host.Host.Services.GetRequiredService<INodeRegistry>().GetLiveInstances("frag-a"));
        Assert.Equal("node-legit", instance.Subject);
    }

    /// <summary>
    /// A node of another identity registers the fragment's name at another version. Counted as a peer,
    /// it would refuse every run of the fragment as misaligned.
    /// </summary>
    [Fact]
    public async Task AForeignClientRegisteringTheFragmentName_DoesNotRefuseTheLegitNodesRun()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);

        var (foreign, foreignLaunched) = await StartNodeAsync(host, Token(jwt, "node-foreign"), "frag-a", "v-other");
        await using var _foreign = foreign;
        var (legit, legitLaunched) = await StartNodeAsync(host, Token(jwt, "node-legit"), "frag-a", "v1");
        await using var _legit = legit;

        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var result = await orchestrator
            .RunAsync(new RunSpec("run-1", [new FragmentPin("frag-a", Subject: "node-legit")], []))
            .WaitAsync(Wait);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Equal("run-1", await legitLaunched.Task.WaitAsync(Wait));
        Assert.False(foreignLaunched.Task.IsCompleted, "the foreign client was launched");
    }

    /// <summary>
    /// Nothing says which node may host the fragment: a hub that attests subjects refuses the run at
    /// once, naming the fragment, instead of admitting whichever instance registered the name.
    /// </summary>
    [Fact]
    public async Task APinThatNamesNoSubject_IsRefusedAtOnce_NamingTheFragment()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt);

        var (node, launched) = await StartNodeAsync(host, Token(jwt, "node-legit"), "frag-a", "v1");
        await using var _node = node;

        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var ex = await Assert.ThrowsAsync<UnboundFragmentException>(
            () => orchestrator.RunAsync(new RunSpec("run-1", [new FragmentPin("frag-a")], [])).WaitAsync(Wait));

        Assert.Equal("frag-a", ex.FragmentName);
        Assert.False(launched.Task.IsCompleted, "a fragment bound to no identity was launched");
    }
}
