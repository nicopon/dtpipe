using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class RunJournalTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "journal-" + Guid.NewGuid().ToString("N"));

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>
    /// A journal file that names no replica and carries pins is read back as it was written: the pins are ignored, and the run
    /// names no replica.
    /// </summary>
    [Fact]
    public void A_Run_That_Names_No_Replica_And_Carries_Pins_Is_Read_Back()
    {
        var runs = Path.Combine(_dir, "runs", "p");
        Directory.CreateDirectory(runs);
        File.WriteAllText(Path.Combine(runs, "run-7.json"), """
            {
              "runId": "run-7", "planId": "plan-1", "state": "succeeded",
              "startedAt": "2026-01-01T00:00:00+00:00", "finishedAt": "2026-01-01T00:00:10+00:00",
              "pins": { "p@n": "v1" },
              "result": { "outcome": "Succeeded", "cause": null, "consequences": [], "reports": {}, "edgeCounts": [], "causeIsUnresponsive": false },
              "description": "Outcome: Succeeded", "refusal": null, "pipelineId": "p", "queuedAt": "2026-01-01T00:00:00+00:00"
            }
            """);
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };

        var journal = new RunJournal(options, NullLogger<RunJournal>.Instance);

        var run = Assert.Single(journal.List());
        Assert.Equal("run-7", run.RunId);
        Assert.Equal(RunOutcome.Succeeded, run.Result!.Outcome);
        Assert.Null(run.Result.Replicas);
        Assert.Equal(8, int.Parse(journal.NextRunId().Split('-').Last()));
    }

    [Fact]
    public void The_Replica_A_Fragment_Ran_On_Is_Kept_With_The_Run()
    {
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };
        var journal = new RunJournal(options, NullLogger<RunJournal>.Instance);
        var result = new RunResult(RunOutcome.Succeeded, null, [], new Dictionary<string, FragmentExitReport>(), [],
            Replicas: new Dictionary<string, RunReplica> { ["p@n"] = new("1a2b3c4d", "host:42") });
        journal.Record(new RunView("run-1", "plan-1", "succeeded", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, result, "ok", null, "p", DateTimeOffset.UtcNow));

        var again = new RunJournal(options, NullLogger<RunJournal>.Instance);

        var replica = Assert.Single(Assert.Single(again.List()).Result!.Replicas!);
        Assert.Equal(("p@n", "1a2b3c4d", "host:42"), (replica.Key, replica.Value.Id, replica.Value.Origin));
    }
}
