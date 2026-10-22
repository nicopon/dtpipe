using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>The kept cursors over a real Redis (<see cref="RedisTestBed"/>), under a prefix of each test's own.</summary>
public sealed class RedisCursorStoreTests : IDisposable
{
    private readonly RedisTestBed _bed = new();

    public void Dispose() => _bed.Dispose();

    [Fact]
    public void A_Cursor_Is_Read_Back_After_A_Restart_Replaced_And_Deleted_Harmlessly()
    {
        RedisTestBed.Need();
        var first = new RedisCursorStore(_bed.Open());
        first.Save("p", "snk", "one");
        first.Save("p", "other", "x");
        first.Save("p", "snk", "two");
        first.Delete("p", "other");
        first.Delete("p", "other");

        // Another start of the coordinator: its own connection, the same prefix.
        var again = new RedisCursorStore(_bed.Open(initialize: false)).Load();

        Assert.Equal([new StoredCursor("p", "snk", "two")], again);
    }
}
