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
        journal.Record(new RunView("run-1", "plan-1", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, result, "ok", null, "p", DateTimeOffset.UtcNow));

        var again = new RunJournal(options, NullLogger<RunJournal>.Instance);

        var replica = Assert.Single(Assert.Single(again.List()).Result!.Replicas!);
        Assert.Equal(("p@n", "1a2b3c4d", "host:42"), (replica.Key, replica.Value.Id, replica.Value.Origin));
    }

    [Fact]
    public void Who_Launched_A_Run_And_The_Key_It_Came_With_Are_Kept_With_The_Run()
    {
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };
        var journal = new RunJournal(options, NullLogger<RunJournal>.Instance);
        journal.Record(new RunView("run-1", "plan-1", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null, "ok", null, "p",
            DateTimeOffset.UtcNow, LaunchedBy: "cron", IdempotencyKey: "night-1"));

        var again = new RunJournal(options, NullLogger<RunJournal>.Instance);

        Assert.Equal("run-1", again.Find(r => r.LaunchedBy == "cron" && r.IdempotencyKey == "night-1")?.RunId);
        Assert.Null(again.Find(r => r.LaunchedBy == "other" && r.IdempotencyKey == "night-1"));
    }

    [Fact]
    public void The_Journal_Keeps_The_Same_Window_At_Start_As_When_It_Records()
    {
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };
        var journal = new RunJournal(options, NullLogger<RunJournal>.Instance);
        var start = DateTimeOffset.UtcNow;
        for (var i = 1; i <= 205; i++)
            journal.Record(new RunView($"run-{i}", "plan", RunState.Succeeded, start.AddSeconds(i), start.AddSeconds(i), null, null, null, "p", start));

        var again = new RunJournal(options, NullLogger<RunJournal>.Instance);

        Assert.Equal(200, again.List(limit: 1000).Count);
        Assert.Equal(200, journal.List(limit: 1000).Count);
        Assert.Null(again.Find(r => r.RunId == "run-5"));
        Assert.Equal("run-205", again.List(limit: 1).Single().RunId);
        Assert.Equal(206, int.Parse(again.NextRunId().Split('-').Last()));
    }

    [Fact]
    public void The_Next_Id_Is_Past_Every_Run_Loaded_Even_One_That_Fell_Out_Of_The_Window()
    {
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };
        var journal = new RunJournal(options, NullLogger<RunJournal>.Instance);
        var start = DateTimeOffset.UtcNow;
        // The highest id started first: it is the oldest by start time, and the one the window drops.
        journal.Record(new RunView("run-999", "plan", RunState.Succeeded, start, start, null, null, null, "p", start));
        for (var i = 1; i <= 200; i++)
            journal.Record(new RunView($"run-{i}", "plan", RunState.Succeeded, start.AddSeconds(i), start.AddSeconds(i), null, null, null, "p", start));

        var again = new RunJournal(options, NullLogger<RunJournal>.Instance);

        Assert.Null(again.Find(r => r.RunId == "run-999"));
        Assert.Equal("run-1000", again.NextRunId());
    }

    [Fact]
    public void The_Plan_A_Run_Ran_Is_Kept_With_The_Run()
    {
        var options = new CoordinatorServerOptions { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };
        var plan = new RunPlan(
            "plan-9", "runner-1", [new RunPlanFragment("p@n", "n", "v1"), new RunPlanFragment("p@runner-1", "runner-1", "v2")],
            [new RunPlanBinding("src", "n", "customers", "b1", "strict", BindingVerdicts.Exact)]);
        new RunJournal(options, NullLogger<RunJournal>.Instance).Record(
            new RunView("run-1", "plan-9", RunState.Succeeded, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null, null, null, "p", DateTimeOffset.UtcNow, plan));

        var back = Assert.Single(new RunJournal(options, NullLogger<RunJournal>.Instance).List());

        Assert.Equal(plan.Runner, back.Plan!.Runner);
        Assert.Equal(plan.Fragments, back.Plan.Fragments);
        Assert.Equal(plan.Bindings, back.Plan.Bindings);
    }

    private CoordinatorServerOptions Options() =>
        new() { StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret" };

    private static RunView InFlight(string id, RunState state, RunPlan? plan = null) =>
        new(id, "plan", state, DateTimeOffset.UtcNow, null, null, null, null, "p", DateTimeOffset.UtcNow, plan, LaunchedBy: "cron", IdempotencyKey: "k-" + id);

    [Fact]
    public void A_Run_In_Flight_Is_On_Disk_And_In_No_History()
    {
        var journal = new RunJournal(Options(), NullLogger<RunJournal>.Instance);

        journal.Record(InFlight("run-1", RunState.Queued));

        Assert.True(File.Exists(Path.Combine(_dir, "runs", "p", "run-1.json")));
        Assert.Empty(journal.List());
        Assert.Null(journal.Find(r => r.RunId == "run-1"));
    }

    [Fact]
    public void A_Restart_Ends_A_Run_In_Flight_Keeps_Its_Key_And_Counts_Its_Id()
    {
        var journal = new RunJournal(Options(), NullLogger<RunJournal>.Instance);
        journal.Record(InFlight("run-4", RunState.Queued));
        journal.Record(InFlight("run-9", RunState.Running, new RunPlan("plan", null, [new RunPlanFragment("p@n", "n", "v1")], [])));

        var again = new RunJournal(Options(), NullLogger<RunJournal>.Instance);

        Assert.All(again.List(), r => Assert.Equal((RunState.Error, RunJournal.RestartedRefusal), (r.State, r.Refusal)));
        Assert.Equal(["run-4", "run-9"], again.List().Select(r => r.RunId).Order());
        Assert.NotNull(again.Find(r => r.IdempotencyKey == "k-run-9")!.FinishedAt);
        Assert.Equal("run-9", Assert.Single(again.Orphans).RunId);
        Assert.Equal("run-10", again.NextRunId());
        // The file says the run is over: a second restart finds no orphan and ends nothing again.
        Assert.Empty(new RunJournal(Options(), NullLogger<RunJournal>.Instance).Orphans);
    }

    [Fact]
    public void A_Running_Run_That_Never_Had_A_Plan_Is_Ended_But_Owes_Nothing_To_A_Node()
    {
        new RunJournal(Options(), NullLogger<RunJournal>.Instance).Record(InFlight("run-1", RunState.Running));

        var again = new RunJournal(Options(), NullLogger<RunJournal>.Instance);

        Assert.Equal(RunState.Error, Assert.Single(again.List()).State);
        Assert.Empty(again.Orphans);
    }
}
