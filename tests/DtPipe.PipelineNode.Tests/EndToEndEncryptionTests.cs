using System.Security.Cryptography;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TransportR.Client;
using TransportR.Hub;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// End-to-end encryption as a replica's client takes it from <see cref="NodeClientOptions"/>, through the coordinator's hub.
/// <c>TransferInfo.Protection</c> at both ends is the proof a deployment has: a transfer in clear reads the same to the two
/// children. Ends set differently never fall back to clear: both runs fail, each as its peer's doing, before a row crosses.
/// </summary>
public class EndToEndEncryptionTests : IDisposable
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private static readonly byte[] DomainKey = RandomNumberGenerator.GetBytes(32);
    private static readonly byte[] OtherKey = RandomNumberGenerator.GetBytes(32);

    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("end-to-end-");

    public void Dispose() => _dir.Delete(recursive: true);

    private static NodeClientOptions Ends(CoordinatorTestHost host, string ends) => ends switch
    {
        "clear" => new() { HubUrl = host.Url },
        "encrypted" => new() { HubUrl = host.Url, EndToEndEncryption = true },
        "encrypted with the key" => new() { HubUrl = host.Url, EndToEndEncryption = true, EndToEndPreSharedKey = _ => DomainKey },
        "encrypted with another key" => new() { HubUrl = host.Url, EndToEndEncryption = true, EndToEndPreSharedKey = _ => OtherKey },
        _ => throw new ArgumentOutOfRangeException(nameof(ends)),
    };

    private static FragmentLaunch Launch(string job, EdgeDirection direction) => new()
    {
        DtPipeExecutable = DtPipeExecutableLocator.Path,
        FragmentJobPath = job,
        Edges = [new EdgeBinding("main", direction)],
    };

    private static ITransferInitiator Initiator(CoordinatorTestHost host) => host.Host.Services.GetRequiredService<ITransferInitiator>();

    /// <summary>The protection one end reports for the transfer, once its handle has reached that end.</summary>
    private static async Task<TransferProtection?> ProtectionAsync(Func<IReadOnlyList<TransferInfo>> transfers, string transferId)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (DateTime.UtcNow < deadline)
        {
            if (transfers().FirstOrDefault(t => t.TransferId == transferId) is { } transfer) return transfer.Protection;
            await Task.Delay(20);
        }
        return null;
    }

    public static TheoryData<string, TransferProtection> AlikeEnds => new()
    {
        { "clear", TransferProtection.None },
        { "encrypted", TransferProtection.Encrypted },
        { "encrypted with the key", TransferProtection.EncryptedWithPreSharedKey },
    };

    [Theory]
    [MemberData(nameof(AlikeEnds))]
    public async Task EndsSetAlike_CarryTheRows_AndBothReportTheProtection(string ends, TransferProtection expected)
    {
        using var fixture = SplitFixture.Create(rowCount: 20_000);
        await using var host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });
        await using var producer = await HandWiredNode.StartAsync(Ends(host, ends), Launch(fixture.ProducerJobPath, EdgeDirection.Outbound));
        await using var consumer = await HandWiredNode.StartAsync(Ends(host, ends), Launch(fixture.ConsumerJobPath, EdgeDirection.Inbound));

        var transferId = await Initiator(host).InitTransferAsync(producer.ClientId, consumer.ClientId, handshakeTimeout: TimeSpan.FromSeconds(20));

        // Read while both handles wait for their run: a transfer that has ended leaves no session to ask.
        Assert.Equal(expected, await ProtectionAsync(() => producer.Transfers, transferId));
        Assert.Equal(expected, await ProtectionAsync(() => consumer.Transfers, transferId));

        await Task.WhenAll(
            producer.WireAsync("main", transferId).WaitAsync(Bound),
            consumer.WireAsync("main", transferId).WaitAsync(Bound));
        var exitCodes = await Task.WhenAll(
            producer.RunToCompletionAsync().WaitAsync(Bound),
            consumer.RunToCompletionAsync().WaitAsync(Bound));

        Assert.Equal([0, 0], exitCodes);
        Assert.Equal(fixture.RowCount, consumer.RowCounts["main"]);
        Assert.Equal(
            File.ReadAllLines(fixture.WitnessCsvPath).Order(StringComparer.Ordinal),
            File.ReadAllLines(fixture.SplitCsvPath).Order(StringComparer.Ordinal));
    }

    public static TheoryData<string, string> DifferentEnds => new()
    {
        { "encrypted with the key", "clear" },
        { "clear", "encrypted with the key" },
        { "encrypted", "clear" },
        { "encrypted with the key", "encrypted" },
        { "encrypted with the key", "encrypted with another key" },
    };

    [Theory]
    [MemberData(nameof(DifferentEnds))]
    public async Task EndsSetDifferently_FailBothRunsAsTheirPeers_BeforeARowCrosses(string producerEnds, string consumerEnds)
    {
        using var fixture = SplitFixture.Create(rowCount: 20_000);
        await using var host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });
        await using var producer = await HandWiredNode.StartAsync(Ends(host, producerEnds), Launch(fixture.ProducerJobPath, EdgeDirection.Outbound));
        await using var consumer = await HandWiredNode.StartAsync(Ends(host, consumerEnds), Launch(fixture.ConsumerJobPath, EdgeDirection.Inbound));

        var transferId = await Initiator(host).InitTransferAsync(producer.ClientId, consumer.ClientId, handshakeTimeout: TimeSpan.FromSeconds(20));
        await Task.WhenAll(
            producer.WireAsync("main", transferId).WaitAsync(Bound),
            consumer.WireAsync("main", transferId).WaitAsync(Bound));
        var exitCodes = await Task.WhenAll(
            producer.RunToCompletionAsync().WaitAsync(Bound),
            consumer.RunToCompletionAsync().WaitAsync(Bound));

        Assert.All(exitCodes, code => Assert.NotEqual(0, code));
        Assert.Equal(FaultOrigin.Remote, producer.Origin);
        Assert.Equal(FaultOrigin.Remote, consumer.Origin);
        Assert.Equal(0, consumer.RowCounts["main"]);
        foreach (var key in new[] { DomainKey, OtherKey }.Select(Convert.ToBase64String))
        {
            Assert.DoesNotContain(key, producer.FirstFault);
            Assert.DoesNotContain(key, consumer.FirstFault);
        }
    }

    [Fact]
    public async Task AKeyWithoutEncryption_IsRefusedBeforeAClientIsBuilt()
    {
        var refused = await Assert.ThrowsAsync<ArgumentException>(() => PipelineNode.ConnectAsync(
            new NodeClientOptions { HubUrl = "http://127.0.0.1:1", EndToEndPreSharedKey = _ => DomainKey },
            ct: TestContext.Current.CancellationToken));
        Assert.Contains("end-to-end encryption is off", refused.Message);
    }

    /// <summary>
    /// Two node hosts read the same key from their own configuration, one as a value and one from a file, and the clients they
    /// register seal the transfer the coordinator opens between them with it. The hosts name no dtpipe binary: nothing runs.
    /// </summary>
    [Fact]
    public async Task NodeHosts_SealTheirTransfersWithTheKeyOfTheirOwnConfiguration()
    {
        var jwt = new TestJwt();
        var rights = new MutableGroups();
        rights.BySubject["node-a"] = ["g"];
        rights.BySubject["node-b"] = ["g"];
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["g"] = new GroupAccess { CanSendTo = ["g"] },
            services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)), jwt);
        await using var idp = await FakeIdp.StartAsync(jwt);
        var keyFile = Path.Combine(_dir.FullName, "domain.key");
        await File.WriteAllTextAsync(keyFile, Convert.ToBase64String(DomainKey) + "\n", TestContext.Current.CancellationToken);

        var a = await StartAgentAsync(host, idp, "node-a", new NodeEncryptionOptions { Enabled = true, PreSharedKey = Convert.ToBase64String(DomainKey) });
        var b = await StartAgentAsync(host, idp, "node-b", new NodeEncryptionOptions { Enabled = true, PreSharedKey = "@" + keyFile });
        try
        {
            var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();
            var transferId = await Initiator(host).InitTransferAsync(
                await AnnouncedClientAsync(registry, "node-a"), await AnnouncedClientAsync(registry, "node-b"),
                handshakeTimeout: TimeSpan.FromSeconds(20));

            Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, await ProtectionAsync(() => a.DataPlane!.Transfers, transferId));
            Assert.Equal(TransferProtection.EncryptedWithPreSharedKey, await ProtectionAsync(() => b.DataPlane!.Transfers, transferId));
        }
        finally
        {
            foreach (var agent in new[] { a, b })
            {
                await agent.StopAsync(CancellationToken.None);
                agent.Dispose();
            }
        }
    }

    private async Task<NodeAgent> StartAgentAsync(CoordinatorTestHost host, FakeIdp idp, string name, NodeEncryptionOptions encryption)
    {
        var options = new DtPipeNodeOptions
        {
            Name = name,
            CoordinatorUrl = host.Url,
            DtPipe = Path.Combine(_dir.FullName, "no-such-dtpipe"),
            StateDirectory = Path.Combine(_dir.FullName, name),
            Oidc = new NodeOidcOptions { Authority = idp.Authority, ClientId = name, ClientSecret = "secret" },
            EndToEndEncryption = encryption,
        };
        var agent = new NodeAgent(Options.Create(options), new NodeDefinition(), NullLoggerFactory.Instance);
        await agent.StartAsync(CancellationToken.None);
        return agent;
    }

    private static async Task<Guid> AnnouncedClientAsync(ReplicaRegistry registry, string node)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (DateTime.UtcNow < deadline)
        {
            if (registry.Replicas(node) is [{ Connected: true, DataPlaneClientId: { } id }]) return id;
            await Task.Delay(100);
        }
        throw new TimeoutException($"{node} never announced a data plane client");
    }
}
