using System.Security.Claims;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

public sealed class EnrollmentQueueTests
{
    private static NodeAnnouncement Host(string name = "proposed", string description = "a host", params string[] bricks) =>
        new(name, description, [], NodeRole.Data, [.. bricks.Select(b => new BrickInfo(b, BrickKind.Source, b, "", "{}", null, null))]);

    private static readonly IReadOnlyDictionary<string, string> NoLabels = new Dictionary<string, string>();

    [Fact]
    public void A_New_Subject_Is_Recorded_Then_Unchanged_Until_It_Proposes_Something_Else()
    {
        var queue = MemoryIgnoredHostStore.Queue();
        Assert.Equal(Proposal.Recorded, queue.Propose("s", Host(), NoLabels));
        Assert.Equal(Proposal.Unchanged, queue.Propose("s", Host(), NoLabels));
        Assert.Equal(Proposal.Changed, queue.Propose("s", Host("other"), NoLabels));
        Assert.Single(queue.List());
        Assert.Equal("other", queue.Find("s")!.ProposedName);
    }

    [Fact]
    public void What_A_Host_Proposes_Is_Cut_And_Stripped_Of_Control_Characters()
    {
        var queue = MemoryIgnoredHostStore.Queue();
        var labels = Enumerable.Range(0, 30).ToDictionary(i => "claim" + i, _ => new string('v', 500));
        queue.Propose("s", Host(new string('n', 500) + "\n", "line one\r\nline two", Enumerable.Range(0, 50).Select(i => new string('b', 300)).ToArray()), labels);
        var pending = queue.Find("s")!;
        Assert.Equal(EnrollmentQueue.MaxName, pending.ProposedName.Length);
        Assert.DoesNotContain('\n', pending.Description);
        Assert.Equal(EnrollmentQueue.MaxBricks, pending.Bricks.Count);
        Assert.All(pending.Bricks, b => Assert.True(b.Length <= EnrollmentQueue.MaxBrick));
        Assert.Equal(EnrollmentQueue.MaxLabels, pending.Labels.Count);
        Assert.All(pending.Labels.Values, v => Assert.True(v.Length <= EnrollmentQueue.MaxLabelValue));
    }

    [Fact]
    public void A_Full_Queue_Refuses_A_New_Subject_Once_And_Evicts_Nobody()
    {
        var queue = MemoryIgnoredHostStore.Queue();
        for (var i = 0; i < EnrollmentQueue.MaxPending; i++) Assert.Equal(Proposal.Recorded, queue.Propose("s" + i, Host(), NoLabels));
        Assert.Equal(Proposal.Full, queue.Propose("late", Host(), NoLabels));
        Assert.Equal(Proposal.FullAgain, queue.Propose("late", Host(), NoLabels));
        Assert.Equal(EnrollmentQueue.MaxPending, queue.List().Count);
        Assert.NotNull(queue.Find("s0"));
        Assert.Null(queue.Find("late"));
        Assert.Equal(Proposal.Unchanged, queue.Propose("s0", Host(), NoLabels));
    }

    [Fact]
    public void An_Ignored_Subject_Stays_Out_Until_The_Lifetime_Ends()
    {
        var time = new FakeTimeProvider();
        var queue = MemoryIgnoredHostStore.Queue(time);
        queue.Propose("s", Host(), NoLabels);
        queue.Ignore("s");
        Assert.Empty(queue.List());
        Assert.Equal(Proposal.Ignored, queue.Propose("s", Host(), NoLabels));
        time.Advance(EnrollmentQueue.Lifetime + TimeSpan.FromMinutes(1));
        Assert.Equal(Proposal.Recorded, queue.Propose("s", Host(), NoLabels));
    }

    [Fact]
    public void An_Ignored_Subject_Is_Still_Ignored_After_A_Restart_Until_The_Lifetime_Ends()
    {
        var time = new FakeTimeProvider();
        var store = new MemoryIgnoredHostStore();
        new EnrollmentQueue(store, time).Ignore("s");

        time.Advance(TimeSpan.FromHours(20));
        var restarted = new EnrollmentQueue(store, time);
        Assert.Equal(Proposal.Ignored, restarted.Propose("s", Host(), NoLabels));

        time.Advance(TimeSpan.FromHours(5));
        Assert.Equal(Proposal.Recorded, restarted.Propose("s", Host(), NoLabels));
    }

    [Fact]
    public void A_Store_That_Refuses_Leaves_The_Host_Waiting_And_Not_Ignored()
    {
        var store = new FailingIgnoredHostStore(new MemoryIgnoredHostStore());
        var queue = new EnrollmentQueue(store);
        queue.Propose("s", Host(), NoLabels);
        store.Down = true;

        Assert.Throws<StoreUnavailableException>(() => queue.Ignore("s"));

        Assert.Single(queue.List());
        Assert.Equal(Proposal.Unchanged, queue.Propose("s", Host(), NoLabels));
    }

    [Fact]
    public void A_Waiting_Subject_Expires_When_Its_Host_Stops_Coming_Back()
    {
        var time = new FakeTimeProvider();
        var queue = MemoryIgnoredHostStore.Queue(time);
        queue.Propose("s", Host(), NoLabels);
        time.Advance(EnrollmentQueue.Lifetime - TimeSpan.FromMinutes(1));
        queue.Propose("s", Host(), NoLabels);
        time.Advance(TimeSpan.FromHours(23));
        Assert.Single(queue.List());
        time.Advance(TimeSpan.FromHours(2));
        Assert.Empty(queue.List());
    }

    [Fact]
    public void A_Subject_With_No_Usable_Key_Is_Invalid()
    {
        var queue = MemoryIgnoredHostStore.Queue();
        Assert.Equal(Proposal.Invalid, queue.Propose(null, Host(), NoLabels));
        Assert.Equal(Proposal.Invalid, queue.Propose("", Host(), NoLabels));
        Assert.Equal(Proposal.Invalid, queue.Propose(new string('s', EnrollmentQueue.MaxSubject + 1), Host(), NoLabels));
    }

    [Fact]
    public void Labels_Are_The_Listed_Claims_Of_The_Token()
    {
        var user = new ClaimsPrincipal(new ClaimsIdentity([new Claim("email", "ops@example.test"), new Claim("sub", "s"), new Claim("team", "data")]));
        var labels = EnrollmentQueue.LabelsOf(user, ["email", "team", "missing"]);
        Assert.Equal(2, labels.Count);
        Assert.Equal("ops@example.test", labels["email"]);
        Assert.Empty(EnrollmentQueue.LabelsOf(user, []));
    }
}
