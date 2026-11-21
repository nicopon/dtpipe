using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Coordinator.Server.Runs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The words a run's state and a deployment's state are written with, by the serializers that write them: the server's
/// own, for what the pages and the event stream read, and the run journal's, for the files it keeps and reads back at
/// start. Every state of both is pinned, in lowercase. The frame the event stream wraps an event in is pinned beside them.
/// </summary>
public sealed class StateWireTests : IDisposable
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly string[] RunWords = ["queued", "running", "succeeded", "failed", "cancelled", "refused", "error"];
    private static readonly string[] DeploymentWords = ["deploying", "ready", "failed"];

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "wire-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private static RunView RunIn(string word, string runId = "run-1") => new(
        runId, "plan-1", Enum.Parse<RunState>(word, ignoreCase: true), DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"), DateTimeOffset.Parse("2026-01-01T00:00:10+00:00"),
        new RunResult(RunOutcome.Succeeded, null, [], new Dictionary<string, FragmentExitReport>(), []), "Outcome: Succeeded", null,
        "p", DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"));

    private static DeploymentView DeploymentIn(string word) => new(
        "p", "plan-1", "test", Enum.Parse<DeploymentState>(word, ignoreCase: true), DateTimeOffset.Parse("2026-01-01T00:00:00+00:00"), null,
        [new DeployedFragment("p@n", "n", "v1", 1)], 0, 0, null);

    /// <summary>The word a state is written with, read here from its name and not from the code under test.</summary>
    private static string WordOf(RunState state) => state.ToString().ToLowerInvariant();

    private static CoordinatorServerOptions OptionsIn(string dir) =>
        new() { StateDir = dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };

    [Fact]
    public void The_Server_Writes_Every_State_Of_A_Run_And_Of_A_Deployment_As_A_Lowercase_Word()
    {
        using var host = new CoordinatorHost();
        using var _ = host.CreateClient();
        var json = host.Services.GetRequiredService<IOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>>().Value.SerializerOptions;

        foreach (var word in RunWords)
        {
            var element = JsonSerializer.SerializeToElement(RunIn(word), json);
            Assert.Equal(word, element.GetProperty("state").GetString());
            Assert.Equal("Succeeded", element.GetProperty("result").GetProperty("outcome").GetString());
            Assert.Equal(word, WordOf(JsonSerializer.Deserialize<RunView>(element, json)!.State));
        }
        foreach (var word in DeploymentWords)
        {
            var element = JsonSerializer.SerializeToElement(DeploymentIn(word), json);
            Assert.Equal(word, element.GetProperty("state").GetString());
        }
    }

    [Fact]
    public async Task The_Pages_Read_The_State_Of_Every_Journaled_Run_As_A_Lowercase_Word()
    {
        using var host = new CoordinatorHost();
        var journal = host.Services.GetRequiredService<RunJournal>();
        foreach (var (word, i) in RunWords.Select((w, i) => (w, i)))
            journal.Record(RunIn(word, $"run-{i + 1}"));
        var client = host.Bearer(CoordinatorHost.Person(CoordinatorHost.Reader));

        // History holds the runs that are over; a run in flight is on disk and in the queue.
        var over = RunWords.Where(w => w is not ("queued" or "running")).Order();
        var runs = await client.GetFromJsonAsync<JsonElement>("/api/runs", Ct);
        Assert.Equal(over, runs.EnumerateArray().Select(r => r.GetProperty("state").GetString()!).Order());

        var snapshot = await client.GetFromJsonAsync<JsonElement>("/api/state", Ct);
        Assert.Equal(over, snapshot.GetProperty("history").EnumerateArray().Select(r => r.GetProperty("state").GetString()!).Order());

        // The journal's filter is a state's word and its total counts the runs that answer the filters, whatever the page holds.
        using var failed = await client.GetAsync("/api/runs?state=failed&limit=1", Ct);
        Assert.Equal("1", Assert.Single(failed.Headers.GetValues("X-Total-Count")));
        Assert.Equal(["failed"], (await failed.Content.ReadFromJsonAsync<JsonElement>(Ct)).EnumerateArray().Select(r => r.GetProperty("state").GetString()));
        using var page = await client.GetAsync("/api/runs?limit=2", Ct);
        Assert.Equal($"{over.Count()}", Assert.Single(page.Headers.GetValues("X-Total-Count")));
        using var nonsense = await client.GetAsync("/api/runs?state=1", Ct);
        Assert.Equal(System.Net.HttpStatusCode.BadRequest, nonsense.StatusCode);

        // A run has an address, and the literal routes beside it keep theirs.
        var one = await client.GetFromJsonAsync<JsonElement>("/api/runs/run-3", Ct);
        Assert.Equal("run-3", one.GetProperty("runId").GetString());
        using var gone = await client.GetAsync("/api/runs/run-99", Ct);
        Assert.Equal(System.Net.HttpStatusCode.NotFound, gone.StatusCode);
        foreach (var literal in new[] { "/api/runs/queue", "/api/runs/last" })
            Assert.Equal(JsonValueKind.Array, (await client.GetFromJsonAsync<JsonElement>(literal, Ct)).ValueKind);
    }

    /// <summary>
    /// The routes that carry a run's result write each end of an edge with the protection it reported, as a word or as null for an
    /// end that reported none, in the record the run already has: no route of its own.
    /// </summary>
    [Fact]
    public async Task The_Run_Routes_Write_The_Protection_Each_End_Of_An_Edge_Reported_As_A_Word_Or_Null()
    {
        using var host = new CoordinatorHost();
        var journal = host.Services.GetRequiredService<RunJournal>();
        var edges = new[] { new EdgeCount("p", "out", "c", "in", 5, 5, DataPlaneProtections.EncryptedPsk, null) };
        journal.Record(RunIn("succeeded") with { Result = new RunResult(RunOutcome.Succeeded, null, [], new Dictionary<string, FragmentExitReport>(), edges) });
        var client = host.Bearer(CoordinatorHost.Person(CoordinatorHost.Reader));

        var routes = new Dictionary<string, JsonElement>
        {
            ["/api/runs/run-1"] = await client.GetFromJsonAsync<JsonElement>("/api/runs/run-1", Ct),
            ["/api/runs/last"] = (await client.GetFromJsonAsync<JsonElement>("/api/runs/last", Ct))[0],
            ["/api/runs"] = (await client.GetFromJsonAsync<JsonElement>("/api/runs", Ct))[0],
        };
        foreach (var run in routes.Values)
        {
            var edge = run.GetProperty("result").GetProperty("edgeCounts")[0];
            Assert.Equal(DataPlaneProtections.EncryptedPsk, edge.GetProperty("producerProtection").GetString());
            Assert.Equal(JsonValueKind.Null, edge.GetProperty("consumerProtection").ValueKind);
        }
    }

    /// <summary>
    /// The routes that carry a run's result write the kind of its cause beside the cause, and the reason the journal derives from it, in
    /// the record the run already has: no route of its own.
    /// </summary>
    [Fact]
    public async Task The_Run_Routes_Write_The_Kind_Of_The_Cause_Beside_It()
    {
        using var host = new CoordinatorHost();
        var journal = host.Services.GetRequiredService<RunJournal>();
        journal.Record(RunIn("failed") with
        {
            Result = new RunResult(RunOutcome.Failed, "p -> c", ["p", "c"], new Dictionary<string, FragmentExitReport>(), [], CauseKind: CauseKinds.Edge),
        });
        var client = host.Bearer(CoordinatorHost.Person(CoordinatorHost.Reader));

        var routes = new Dictionary<string, JsonElement>
        {
            ["/api/runs/run-1"] = await client.GetFromJsonAsync<JsonElement>("/api/runs/run-1", Ct),
            ["/api/runs/last"] = (await client.GetFromJsonAsync<JsonElement>("/api/runs/last", Ct))[0],
            ["/api/runs"] = (await client.GetFromJsonAsync<JsonElement>("/api/runs", Ct))[0],
        };
        foreach (var run in routes.Values)
        {
            var result = run.GetProperty("result");
            Assert.Equal(("p -> c", CauseKinds.Edge), (result.GetProperty("cause").GetString(), result.GetProperty("causeKind").GetString()));
            Assert.StartsWith("The ends of p -> c are set differently", run.GetProperty("reason").GetString());
        }
    }

    /// <summary>
    /// The route of the nodes writes, under the names the Nodes page reads, the level and key id each replica announced (a key id present
    /// as null when there is none), whether it is under the level the deployment requires, away or connected, and for each node
    /// whether its connected replicas differ in level or key, and the level required. The route renders the inventory's snapshot as it
    /// is: the replicas reach it through the registry and the inventory, as a host's announcement does.
    /// </summary>
    [Fact]
    public async Task The_Nodes_Route_Writes_The_Protection_Of_Each_Replica_Under_The_Names_The_Nodes_Page_Reads()
    {
        const string KeyA = "0123456789abcdef", KeyB = "fedcba9876543210";
        using var host = new CoordinatorHost(settings: new Dictionary<string, string> { ["DataPlane:RequiredProtection"] = DataPlaneProtections.EncryptedPsk });
        var registry = host.Services.GetRequiredService<ReplicaRegistry>();
        var inventory = host.Services.GetRequiredService<NodeInventory>();

        // The replica whose instance id starts with {n:x8}, which is the id the route shows it under.
        void Announce(string node, int n, string level, string? keyId = null, bool away = false)
        {
            var announcement = new NodeAnnouncement(
                node, "", [], NodeRole.Data, [], InstanceId: Guid.Parse($"{n:x8}-0000-0000-0000-000000000000"), DataPlaneProtection: level, KeyId: keyId);
            registry.Announce($"c{n}", node, announcement, ["g"], () => { });
            inventory.Announced(node, node, announcement, ["g"]);
            if (away) registry.Disconnected($"c{n}");
        }

        // The levels differ; the key differs under one level; the connected replicas agree while one that is away does not.
        Announce("mixed", 1, "encrypted-psk", KeyA);
        Announce("mixed", 2, "none");
        Announce("rekeyed", 3, "encrypted-psk", KeyA);
        Announce("rekeyed", 4, "encrypted-psk", KeyB);
        Announce("alike", 5, "encrypted-psk", KeyA);
        Announce("alike", 6, "encrypted-psk", KeyA);
        Announce("alike", 7, "encrypted", away: true);

        var nodes = (await host.Bearer(CoordinatorHost.Person(CoordinatorHost.Reader)).GetFromJsonAsync<JsonElement>("/api/nodes", Ct))
            .EnumerateArray().ToDictionary(n => n.GetProperty("name").GetString()!);

        Assert.Equal(["alike", "mixed", "rekeyed"], nodes.Keys.Order());
        foreach (var node in nodes.Values)
            Assert.Equal("encrypted-psk", node.GetProperty("requiredProtection").GetString());
        Assert.Equal(
            [("alike", false), ("mixed", true), ("rekeyed", true)],
            nodes.OrderBy(n => n.Key).Select(n => (n.Key, n.Value.GetProperty("replicasDisagreeOnProtection").GetBoolean())));
        Assert.All(nodes.Values, n => Assert.False(n.GetProperty("replicasDisagree").GetBoolean()));

        var replicas = nodes.Values.SelectMany(n => n.GetProperty("replicas").EnumerateArray()).ToDictionary(r => r.GetProperty("id").GetString()!);
        var shown = replicas.OrderBy(r => r.Key).Select(r => (
            r.Key,
            r.Value.GetProperty("connected").GetBoolean(),
            r.Value.GetProperty("dataPlaneProtection").GetString(),
            r.Value.GetProperty("keyId").GetString(),
            r.Value.GetProperty("belowRequiredProtection").GetBoolean()));
        Assert.Equal(
            [
                ("00000001", true, "encrypted-psk", KeyA, false),
                ("00000002", true, "none", null, true),
                ("00000003", true, "encrypted-psk", KeyA, false),
                ("00000004", true, "encrypted-psk", KeyB, false),
                ("00000005", true, "encrypted-psk", KeyA, false),
                ("00000006", true, "encrypted-psk", KeyA, false),
                ("00000007", false, "encrypted", null, true),
            ],
            shown);
        // A replica with no key carries the name all the same, as null: the page reads it as no key.
        Assert.Equal(JsonValueKind.Null, replicas["00000002"].GetProperty("keyId").ValueKind);
    }

    /// <summary>An event of the bus leaves with its number and the bus's id, a line replayed from the backlog says so and a live one does not, and the id is the same from one connection to the next.</summary>
    [Fact]
    public async Task The_Event_Stream_Numbers_The_Events_Of_The_Bus_And_Marks_The_Lines_It_Replays()
    {
        using var host = new CoordinatorHost();
        var bus = host.Services.GetRequiredService<EventBus>();
        var client = host.Bearer(CoordinatorHost.Person(CoordinatorHost.Reader));
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));

        async Task<JsonElement> Next(StreamReader reader)
        {
            while (await reader.ReadLineAsync(timeout.Token) is { } line)
                if (line.StartsWith("data: ", StringComparison.Ordinal)) return JsonSerializer.Deserialize<JsonElement>(line["data: ".Length..]);
            throw new EndOfStreamException("The stream ended before the frame this test waits for.");
        }

        async Task<JsonElement> LogLine(StreamReader reader, string message)
        {
            while (true)
            {
                var frame = await Next(reader);
                if (frame.GetProperty("type").GetString() == "log" && frame.GetProperty("data").GetProperty("message").GetString() == message) return frame;
            }
        }

        bus.Publish("log", new NodeLogLine("coordinator", null, "Information", "before"));

        string boot;
        long beforeSeq;
        using (var first = await client.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, timeout.Token))
        using (var reader = new StreamReader(await first.Content.ReadAsStreamAsync(timeout.Token)))
        {
            // The three snapshots come from no event of the bus.
            foreach (var type in new[] { "nodes", "state", "deployments" })
            {
                var snapshot = await Next(reader);
                Assert.Equal(type, snapshot.GetProperty("type").GetString());
                Assert.False(snapshot.TryGetProperty("seq", out _) || snapshot.TryGetProperty("boot", out _) || snapshot.TryGetProperty("replay", out _));
            }

            var replayed = await LogLine(reader, "before");
            Assert.True(replayed.GetProperty("replay").GetBoolean());
            boot = replayed.GetProperty("boot").GetString()!;
            Assert.False(string.IsNullOrEmpty(boot));
            beforeSeq = replayed.GetProperty("seq").GetInt64();

            bus.Publish("log", new NodeLogLine("coordinator", null, "Information", "after"));
            var live = await LogLine(reader, "after");
            Assert.False(live.TryGetProperty("replay", out _));
            Assert.Equal(boot, live.GetProperty("boot").GetString());
            Assert.True(live.GetProperty("seq").GetInt64() > beforeSeq);
        }

        using var second = await client.GetAsync("/api/events", HttpCompletionOption.ResponseHeadersRead, timeout.Token);
        using var again = new StreamReader(await second.Content.ReadAsStreamAsync(timeout.Token));
        var replayedAgain = await LogLine(again, "before");
        Assert.Equal(boot, replayedAgain.GetProperty("boot").GetString());
        Assert.Equal(beforeSeq, replayedAgain.GetProperty("seq").GetInt64());
        Assert.True(replayedAgain.GetProperty("replay").GetBoolean());
    }

    [Fact]
    public void The_Journal_Keeps_The_State_Of_A_Run_As_A_Lowercase_Word_In_Its_Document_And_Reads_Every_Such_Document_Back()
    {
        var journal = MemoryRunStore.Journal(OptionsIn(_dir));
        foreach (var (word, i) in RunWords.Select((w, i) => (w, i)))
            journal.Record(RunIn(word, $"run-{i + 1}"));

        foreach (var (word, i) in RunWords.Select((w, i) => (w, i)))
            Assert.Contains($"\"state\":\"{word}\"", MemoryRunStore.At(_dir).Document($"run-{i + 1}"));

        // A restart ends a run that was queued or running.
        var again = MemoryRunStore.Journal(OptionsIn(_dir));
        Assert.Equal(RunWords.Select(w => w is "queued" or "running" ? "error" : w).Order(), again.List().Select(r => WordOf(r.State)).Order());
        Assert.Equal("run-8", again.NextRunId());
    }

    /// <summary>A document whose state is none of the seven, or a number, cannot be read: it is skipped, and the others are read and the journal starts.</summary>
    [Fact]
    public void A_Journal_Document_Whose_State_Is_None_Of_The_Seven_Or_A_Number_Is_Skipped_And_The_Others_Are_Read()
    {
        var store = MemoryRunStore.At(_dir);
        foreach (var (id, state) in new[] { ("run-1", "\"succeeded\""), ("run-2", "\"paused\""), ("run-3", "3") })
            store.Seed(id, $$"""
                {
                  "runId": "{{id}}", "planId": "plan-1", "state": {{state}},
                  "startedAt": "2026-01-01T00:00:00+00:00", "finishedAt": "2026-01-01T00:00:10+00:00",
                  "result": null, "description": null, "refusal": null, "pipelineId": "p", "queuedAt": "2026-01-01T00:00:00+00:00"
                }
                """, over: true);

        var journal = MemoryRunStore.Journal(OptionsIn(_dir));

        Assert.Equal("run-1", Assert.Single(journal.List()).RunId);
    }
}
