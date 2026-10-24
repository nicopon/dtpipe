using System.Security.Cryptography;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The stores over a real database of each provider (<see cref="SqlTestBed"/>): what the coordinator keeps when its state is not in
/// Redis. A "start" of the coordinator ends the earlier one, since the database is held by one coordinator at a time.
/// </summary>
public sealed class SqlStoreTests : IDisposable
{
    private static readonly IReadOnlyDictionary<string, string> NoLabels = new Dictionary<string, string>();
    private static readonly NodeAnnouncement Host = new("proposed", "a host", [], NodeRole.Data, []);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "sql-store-" + Guid.NewGuid().ToString("N"));
    private SqlTestBed? _bed;

    public static TheoryData<string> Providers => SqlTestBed.Providers;

    public void Dispose()
    {
        _bed?.Dispose();
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private SqlTestBed Bed(string provider) => _bed ??= new SqlTestBed(provider, _dir);

    private CoordinatorServerOptions Options(string? seed = null) => new()
    {
        StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
    };

    private string Seed()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """
            { "identities": [ { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] } ], "matrix": { "crm": ["crm"] } }
            """);
        return seed;
    }

    private static RunView Over(string id, DateTimeOffset at, string? key = null) =>
        new(id, "plan", RunState.Succeeded, at, at, null, null, null, "p", at, LaunchedBy: "cron", IdempotencyKey: key);

    private RunJournal Journal(SqlTestBed bed, bool initialize = true) => new(new SqlRunStore(bed.Start(initialize)), NullLogger<RunJournal>.Instance);

    [Theory]
    [MemberData(nameof(Providers))]
    public void The_Rights_Are_Read_Back_After_A_Restart_And_The_Seed_Is_Used_Once(string provider)
    {
        var bed = Bed(provider);
        var first = new RightsStore(new SqlRightsStore(bed.Start()), Options(Seed()));
        first.UpdateIdentity("a", null, "Changed", ["crm", "sales"], null, "someone@test");

        var again = new RightsStore(new SqlRightsStore(bed.Start(initialize: false)), Options(Seed()));

        Assert.Equal(["crm", "sales"], again.Find("a")!.Groups);
        Assert.Equal(2, again.Snapshot().Audit!.Count);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Rights_That_Are_Gone_Are_Not_Begun_Again_Unless_Told_To(string provider)
    {
        var failure = Assert.Throws<InvalidOperationException>(() => new RightsStore(new SqlRightsStore(Bed(provider).Start(initialize: false)), Options(Seed())));

        Assert.Contains("Coordinator:Initialize", failure.Message);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void A_Rights_Write_Names_The_Version_It_Read_And_One_Overtaken_Is_Refused(string provider)
    {
        var store = new SqlRightsStore(Bed(provider).Start());
        Assert.True(store.Create("one"));
        Assert.False(store.Create("other"));

        Assert.Equal(2, store.Replace("two", 1));
        Assert.Null(store.Replace("late", 1));
        Assert.Equal(new StoredRights("two", 2), store.Load());
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void An_Id_Is_Never_Given_Again_After_A_Restart(string provider)
    {
        var bed = Bed(provider);
        var journal = Journal(bed);
        Assert.Equal(["run-1", "run-2", "run-3"], new[] { journal.NextRunId(), journal.NextRunId(), journal.NextRunId() });

        Assert.Equal("run-4", Journal(bed, initialize: false).NextRunId());
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void A_Counter_That_Is_Gone_While_Runs_Remain_Is_Raised_To_Their_Highest_Id_And_Never_Lowered(string provider)
    {
        var bed = Bed(provider);
        Journal(bed).Record(Over("run-41", DateTimeOffset.UtcNow));
        bed.Execute("DELETE FROM counters");

        Assert.Equal("run-42", Journal(bed, initialize: false).NextRunId());

        // A floor under the counter leaves it where it is.
        new SqlRunStore(bed.Start()).RaiseSequenceTo(7);
        Assert.Equal("run-43", Journal(bed, initialize: false).NextRunId());
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void A_Store_With_No_Counter_And_No_Run_Refuses_To_Start_Unless_It_Is_Told_To_Initialize(string provider)
    {
        var bed = Bed(provider);
        var refused = Assert.Throws<InvalidOperationException>(() => Journal(bed, initialize: false));
        Assert.Contains("Coordinator:Initialize", refused.Message);

        Assert.Equal("run-1", Journal(bed, initialize: true).NextRunId());
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void A_Key_Is_Found_After_A_Restart_And_A_Run_In_Flight_Is_Ended_With_Its_Key(string provider)
    {
        var bed = Bed(provider);
        var journal = Journal(bed);
        var now = DateTimeOffset.UtcNow;
        journal.Record(Over("run-1", now, "night-1"));
        journal.Record(new RunView("run-2", "plan", RunState.Queued, now, null, null, null, null, "p", now, LaunchedBy: "cron", IdempotencyKey: "night-2"));

        var again = Journal(bed, initialize: false);

        Assert.Equal(RunState.Succeeded, again.Find(r => r.LaunchedBy == "cron" && r.IdempotencyKey == "night-1")!.State);
        var ended = again.Find(r => r.IdempotencyKey == "night-2")!;
        Assert.Equal((RunState.Error, RunJournal.RestartedRefusal), (ended.State, ended.Refusal));
        Assert.All(Journal(bed, initialize: false).List(), r => Assert.True(r.IsOver));
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void An_Abandonment_Owed_Survives_A_Restart_Until_It_Is_Settled_Or_Falls_Due(string provider)
    {
        var bed = Bed(provider);
        var kept = new OwedFragment("run-1", "p@n", "n");
        var gone = new OwedFragment("run-2", "q@m", "m");
        var now = DateTimeOffset.UtcNow;
        var store = new SqlRunStore(bed.Start());
        store.AddOwed([kept, gone], now.AddHours(1));
        store.AddOwed([new OwedFragment("run-3", "r@n", "n")], now.AddSeconds(-1));

        store = new SqlRunStore(bed.Start());
        Assert.Equal([kept, gone], store.ReadOwed(now).OrderBy(o => o.RunId));

        store.RemoveOwed(gone);
        Assert.Equal([kept], new SqlRunStore(bed.Start()).ReadOwed(now));
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void The_History_Keeps_The_Latest_Two_Hundred_And_Drops_The_Documents_It_Cuts(string provider)
    {
        var bed = Bed(provider);
        var journal = Journal(bed);
        var start = DateTimeOffset.UtcNow;
        for (var i = 1; i <= 205; i++) journal.Record(Over($"run-{i}", start.AddSeconds(i)));

        var again = Journal(bed, initialize: false);

        Assert.Equal(200, again.List(limit: 1000).Count);
        Assert.Null(again.Find(r => r.RunId == "run-5"));
        Assert.Equal("run-205", again.List(limit: 1).Single().RunId);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void A_Host_Set_Aside_Stays_Out_After_A_Restart_Until_Its_Lifetime_Ends(string provider)
    {
        var bed = Bed(provider);
        var time = new FakeTimeProvider();
        new EnrollmentQueue(new SqlIgnoredHostStore(bed.Start()), time).Ignore("s");

        time.Advance(TimeSpan.FromHours(20));
        Assert.Equal(Proposal.Ignored, new EnrollmentQueue(new SqlIgnoredHostStore(bed.Start()), time).Propose("s", Host, NoLabels));

        time.Advance(TimeSpan.FromHours(5));
        Assert.Equal(Proposal.Recorded, new EnrollmentQueue(new SqlIgnoredHostStore(bed.Start()), time).Propose("s", Host, NoLabels));
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void The_Set_Of_Hosts_Is_Bounded_And_The_Soonest_To_Be_Let_Back_In_Goes_First(string provider)
    {
        var store = new SqlIgnoredHostStore(Bed(provider).Start());
        var now = new DateTimeOffset(2026, 10, 18, 12, 0, 0, TimeSpan.Zero);

        foreach (var (subject, hours) in new[] { ("a", 1), ("b", 2), ("c", 3) }) store.Add(subject, now.AddHours(hours), now, max: 2);

        Assert.Equal(["b", "c"], store.Load(now).Keys.Order());
        store.Add("d", now.AddHours(10), now.AddHours(2.5), max: 2);
        Assert.Equal(["c", "d"], store.Load(now).Keys.Order());
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void The_Cursors_Are_Kept_Per_Pipeline_And_Branch_Replaced_And_Deleted_Harmlessly(string provider)
    {
        var bed = Bed(provider);
        var store = new SqlCursorStore(bed.Start());
        store.Save("p", "snk", "one");
        store.Save("q", "snk", "other");
        store.Save("p", "snk", "two");
        // A key is compared as it is written: an alias that differs by its case is another branch.
        store.Save("p", "SNK", "upper");
        store.Delete("q", "snk");
        store.Delete("q", "snk");

        Assert.Equal([new StoredCursor("p", "SNK", "upper"), new StoredCursor("p", "snk", "two")],
            new SqlCursorStore(bed.Start()).Load().OrderBy(c => c.Alias, StringComparer.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void The_Armed_Deployments_Are_Kept_Replaced_And_Deleted_Harmlessly(string provider)
    {
        var bed = Bed(provider);
        var store = new SqlDeploymentStore(bed.Start());
        store.Save("p", "one");
        store.Save("q", "two");
        store.Save("p", "three");
        store.Delete("q");
        store.Delete("q");

        Assert.Equal([new StoredDeployment("p", "three")], new SqlDeploymentStore(bed.Start()).Load());
    }

    private static ServiceProvider Keys(SqlDatabase database)
    {
        var providers = new ServiceCollection().AddLogging().AddSessionKeys()
            .AddSingleton<Microsoft.AspNetCore.DataProtection.Repositories.IXmlRepository>(new SqlXmlRepository(database)).BuildServiceProvider();
        TrustedIdp.EnsureSessionKeys(providers);
        return providers;
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void A_Cookie_One_Instance_Issued_Is_Read_By_The_Next_Instance_From_The_Database_Alone(string provider)
    {
        var bed = Bed(provider);
        string cookie;
        using (var first = Keys(bed.Start())) cookie = first.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookie").Protect("session");

        using var next = Keys(bed.Start());
        Assert.Equal("session", next.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookie").Unprotect(cookie));

        // The keys come from the database, not from this machine's profile: an instance on another database cannot read the cookie.
        using var other = Keys(bed.StartOther());
        Assert.Throws<CryptographicException>(() => other.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookie").Unprotect(cookie));
    }

    public static TheoryData<string, string> DriverFailures =>
        new MatrixTheoryData<string, string>(SqlTestBed.Names, ["timeout", "cancelled", "task cancelled"]);

    [Theory]
    [MemberData(nameof(DriverFailures))]
    public void A_Driver_That_Times_Out_Or_Is_Cancelled_Fails_As_The_Store_And_A_Refusal_Of_The_Store_Passes(string provider, string failure)
    {
        var database = Bed(provider).Start();
        Exception thrown = failure switch
        {
            "timeout" => new TimeoutException(),
            "cancelled" => new OperationCanceledException(),
            _ => new TaskCanceledException(),
        };

        var refused = Assert.Throws<StoreUnavailableException>(() => database.Call<int>("the test", (_, _) => throw thrown));
        Assert.Contains("the test", refused.Message);
        Assert.Null(refused.InnerException);

        // What a store refuses on its own account is no failure of the database.
        Assert.Throws<InvalidOperationException>(() => database.Call<int>("the test", (_, _) => throw new InvalidOperationException("refused")));
        Assert.Equal([], new SqlDeploymentStore(database).Load());
    }

    [Fact]
    public void A_Database_Of_A_Newer_Server_Is_Refused()
    {
        var bed = Bed("sqlite");
        bed.Start();
        bed.Execute("UPDATE schema_version SET version = version + 1");

        var refused = Assert.Throws<InvalidOperationException>(() => bed.Start());

        Assert.Contains("newer version", refused.Message);
    }

    [Fact]
    public void A_Second_Coordinator_On_The_Same_File_Is_Refused_Naming_It()
    {
        var first = Bed("sqlite").Start();
        new SqlDeploymentStore(first).Save("p", "kept");

        var refused = Assert.Throws<InvalidOperationException>(() => SqlDatabase.Open(new SqliteDialect(_dir), true, NullLogger.Instance));

        Assert.Contains(SqliteDialect.FileName, refused.Message);
        Assert.Contains("another process", refused.Message);
        // The first is untouched, and the log needs no shared memory file beside the database.
        Assert.Equal([new StoredDeployment("p", "kept")], new SqlDeploymentStore(first).Load());
        Assert.Empty(Directory.GetFiles(_dir, "*-shm"));
    }

    [Fact]
    public void The_Database_Is_Where_The_Directory_Setting_Puts_It_And_In_The_State_Directory_Otherwise()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Coordinator:Authority"] = "http://idp", ["Coordinator:Ui:ClientSecret"] = "x", ["Coordinator:StateDirectory"] = _dir,
        }).Build();
        var options = CoordinatorServerOptions.FromConfiguration(configuration);
        Assert.Equal(StoreKind.Sqlite, options.Store);
        Assert.Null(options.SqliteDirectory);

        var volume = Path.Combine(_dir, "volume");
        configuration["Coordinator:Sqlite:Directory"] = volume;
        Assert.Equal(volume, CoordinatorServerOptions.FromConfiguration(configuration).SqliteDirectory);

        using var database = SqlDatabase.Open(new SqliteDialect(volume), true, NullLogger.Instance);
        Assert.True(File.Exists(Path.Combine(volume, SqliteDialect.FileName)));
    }

    [Theory]
    [InlineData("Coordinator:Redis:ConnectionString", "localhost")]
    [InlineData("Coordinator:Redis:KeyPrefix", "x:")]
    public void Redis_Settings_Under_The_Sqlite_Store_Stop_The_Start_Instead_Of_Being_Ignored(string key, string value)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Coordinator:Authority"] = "http://idp", ["Coordinator:Ui:ClientSecret"] = "x", [key] = value,
        }).Build();

        var failure = Assert.Throws<InvalidOperationException>(() => CoordinatorServerOptions.FromConfiguration(configuration));
        Assert.Contains("Coordinator:Store", failure.Message);
        Assert.DoesNotContain(value, failure.Message);

        configuration["Coordinator:Store"] = "redis";
        Assert.Equal(StoreKind.Redis, CoordinatorServerOptions.FromConfiguration(configuration).Store);
        configuration["Coordinator:Store"] = "memory";
        Assert.Throws<InvalidOperationException>(() => CoordinatorServerOptions.FromConfiguration(configuration));
    }
}
