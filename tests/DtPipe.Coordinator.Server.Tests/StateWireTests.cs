using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The words a run's state and a deployment's state are written with, by the serializers that write them: the server's
/// own, for what the pages and the event stream read, and the run journal's, for the files it keeps and reads back at
/// start. Every state of both is pinned, in lowercase.
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
