using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>The armed deployments over a real Redis (<see cref="RedisTestBed"/>), under a prefix of each test's own.</summary>
public sealed class RedisDeploymentStoreTests : IDisposable
{
    private readonly RedisTestBed _bed = new();

    public void Dispose() => _bed.Dispose();

    [Fact]
    public void A_Deployment_Is_Read_Back_After_A_Restart_And_A_Replaced_One_Is_The_Latest()
    {
        RedisTestBed.Need();
        var first = new RedisDeploymentStore(_bed.Open());
        first.Save("p", "one");
        first.Save("q", "other");
        first.Save("p", "two");

        // Another start of the coordinator: its own connection, the same prefix.
        var again = new RedisDeploymentStore(_bed.Open(initialize: false)).Load().OrderBy(d => d.PipelineId).ToList();

        Assert.Equal([new StoredDeployment("p", "two"), new StoredDeployment("q", "other")], again);
    }

    [Fact]
    public void A_Deleted_Deployment_Leaves_Nothing_And_Deleting_It_Again_Is_Harmless()
    {
        RedisTestBed.Need();
        var store = new RedisDeploymentStore(_bed.Open());
        store.Save("p", "one");

        store.Delete("p");
        store.Delete("p");

        Assert.Empty(store.Load());
    }

    [Fact]
    public void A_Store_That_Cannot_Be_Reached_Refuses_And_The_Armed_Deployments_Stay()
    {
        RedisTestBed.Need();
        var kept = new RedisDeploymentStore(_bed.Open());
        kept.Save("p", "one");
        var connection = _bed.Open();
        var store = new RedisDeploymentStore(connection);
        // Redis goes away: the connection is closed under the store.
        connection.Dispose();

        Assert.Throws<StoreUnavailableException>(() => store.Save("p", "lost"));
        Assert.Throws<StoreUnavailableException>(() => store.Delete("p"));
        Assert.Throws<StoreUnavailableException>(store.Load);

        Assert.Equal([new StoredDeployment("p", "one")], kept.Load());
    }
}
