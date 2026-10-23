using DtPipe.Sessions;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

public class SessionStoreTests : IDisposable
{
    private readonly string _tmp;

    public SessionStoreTests()
    {
        _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_store_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tmp);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
    }

    private SessionStore NewStore(string name = "s1")
        => new(new SessionIdentity(name, Path.Combine(_tmp, ".dtpipe"), SessionOrigin.Explicit));

    [Fact]
    public void The_Store_Does_Not_Exist_Until_Something_Is_Materialised()
    {
        var store = NewStore();

        store.Exists.Should().BeFalse();
        Directory.Exists(store.RootPath).Should().BeFalse();
    }

    [Fact]
    public void Creating_The_Root_Writes_A_Gitignore_That_Ignores_Everything()
    {
        var store = NewStore();

        store.EnsureCreated();

        var gitignore = Path.Combine(store.RootPath, ".gitignore");
        File.Exists(gitignore).Should().BeTrue(
            "the store must ignore itself whatever the project's own .gitignore says — .dtpipe/ is not ignored by default");
        File.ReadAllText(gitignore).Trim().Should().Be("*");
    }

    [Fact]
    public void An_Existing_Gitignore_Is_Not_Overwritten()
    {
        var store = NewStore();
        Directory.CreateDirectory(store.RootPath);
        File.WriteAllText(Path.Combine(store.RootPath, ".gitignore"), "# mine\n*\n");

        store.EnsureCreated();

        File.ReadAllText(Path.Combine(store.RootPath, ".gitignore")).Should().Contain("# mine");
    }

    [Fact]
    public void Reuse_Extends_The_Session_Rather_Than_Restarting_It()
    {
        var store = NewStore();
        var first = store.EnsureCreated();
        var createdAt = first.CreatedAt;

        Thread.Sleep(10);
        var second = store.EnsureCreated();

        second.CreatedAt.Should().Be(createdAt, "the session is prolonged from run to run, not recreated");
        DateTime.Parse(second.LastUsedAt!).Should().BeAfter(DateTime.Parse(first.CreatedAt!));
    }

    [Fact]
    public void Corrupt_Metadata_Is_Replaced_Rather_Than_Fatal()
    {
        var store = NewStore();
        store.EnsureCreated();
        File.WriteAllText(store.MetadataPath, "{ not json");

        var act = () => store.EnsureCreated();

        act.Should().NotThrow("nothing in a session store is irreplaceable, so a corrupt file must not fail a run");
        store.ReadMetadata().Should().NotBeNull();
    }

    [Fact]
    public void Sessions_Are_Isolated_From_Each_Other()
    {
        NewStore("alpha").EnsureCreated();
        NewStore("beta").EnsureCreated();

        SessionStore.EnumerateSessionPaths(Path.Combine(_tmp, ".dtpipe"))
            .Select(Path.GetFileName).Should().BeEquivalentTo(["alpha", "beta"]);
    }
}
