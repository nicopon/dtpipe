using System.Security.Cryptography;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
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

    public static TheoryData<string, TransferProtection, string> NodeSettings => new()
    {
        { "a key, as a value and as a file", TransferProtection.EncryptedWithPreSharedKey, "encrypted-psk" },
        { "no key", TransferProtection.Encrypted, "encrypted" },
        { "encryption off", TransferProtection.None, "none" },
    };

    /// <summary>
    /// Two node hosts take their setting from their own configuration, and the clients they register carry the transfer the
    /// coordinator opens between them as that setting says. What each announces to the coordinator is what its client proves: the
    /// level, and for a key the same fingerprint from a value and from a file. The hosts name no dtpipe binary: nothing runs.
    /// </summary>
    [Theory]
    [MemberData(nameof(NodeSettings))]
    public async Task NodeHosts_AnnounceTheProtectionTheirClientsApply(string setting, TransferProtection proven, string announcedWord)
    {
        var jwt = new TestJwt();
        var rights = new MutableGroups();
        rights.BySubject["node-a"] = ["g"];
        rights.BySubject["node-b"] = ["g"];
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["g"] = new GroupAccess { CanSendTo = ["g"] },
            services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)), jwt);
        await using var idp = await FakeIdp.StartAsync(jwt);
        var keyBase64 = Convert.ToBase64String(DomainKey);
        var keyFile = Path.Combine(_dir.FullName, "domain.key");
        await File.WriteAllTextAsync(keyFile, keyBase64 + "\n", TestContext.Current.CancellationToken);
        NodeEncryptionOptions Setting(string node) => setting switch
        {
            "a key, as a value and as a file" => new() { Enabled = true, PreSharedKey = node == "node-a" ? keyBase64 : "@" + keyFile },
            "no key" => new() { Enabled = true },
            _ => new(),
        };

        var lines = new Lines();
        using var loggers = LoggerFactory.Create(b => b.AddProvider(lines).SetMinimumLevel(LogLevel.Information));
        var a = await StartAgentAsync(host, idp, "node-a", Setting("node-a"), loggers);
        var b = await StartAgentAsync(host, idp, "node-b", Setting("node-b"), loggers);
        try
        {
            var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();
            var transferId = await Initiator(host).InitTransferAsync(
                await AnnouncedClientAsync(registry, "node-a"), await AnnouncedClientAsync(registry, "node-b"),
                handshakeTimeout: TimeSpan.FromSeconds(20));

            Assert.Equal(proven, await ProtectionAsync(() => a.DataPlane!.Transfers, transferId));
            Assert.Equal(proven, await ProtectionAsync(() => b.DataPlane!.Transfers, transferId));

            // The id is the one the derivation gives, which the key itself never appears in.
            var id = proven == TransferProtection.EncryptedWithPreSharedKey
                ? Convert.ToHexString(HMACSHA256.HashData(DomainKey, "dtpipe end-to-end key id"u8.ToArray())[..8]).ToLowerInvariant()
                : null;
            foreach (var node in new[] { "node-a", "node-b" })
            {
                var replica = Assert.Single(registry.Replicas(node));
                Assert.Equal(announcedWord, replica.DataPlaneProtection);
                Assert.Equal(id, replica.KeyId);
                var line = Assert.Single(lines.All, l => l.Contains($"Node {node} data plane protection"));
                Assert.Equal($"Information: Node {node} data plane protection: {announcedWord}" + (id is null ? "" : $", key id {id}"), line);
            }
            Assert.DoesNotContain(lines.All, l => l.Contains(keyBase64) || l.Contains(Convert.ToHexString(DomainKey), StringComparison.OrdinalIgnoreCase) || l.Contains(keyFile));
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

    /// <summary>Every line the node hosts log, as <c>Level: message</c>.</summary>
    private sealed class Lines : ILoggerProvider
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> _lines = new();

        public IReadOnlyList<string> All => [.. _lines];

        public ILogger CreateLogger(string categoryName) => new Line(_lines);

        public void Dispose() { }

        private sealed class Line(System.Collections.Concurrent.ConcurrentQueue<string> lines) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                lines.Enqueue($"{logLevel}: {formatter(state, exception)}");
        }
    }

    private async Task<NodeAgent> StartAgentAsync(CoordinatorTestHost host, FakeIdp idp, string name, NodeEncryptionOptions encryption, ILoggerFactory loggers)
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
        var agent = new NodeAgent(Options.Create(options), new NodeDefinition(), loggers);
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
