using DtPipe.Sessions;
using AwesomeAssertions;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

/// <summary>
/// The precedence chain is a chain because each link fails for someone: a flag nobody wants to
/// repeat, an environment variable that does not survive a new shell, an ancestor that does not
/// exist yet, a directory that cannot be written to. The order is the design.
/// </summary>
[Collection(SessionStateCollection.Name)]
public class SessionIdentityTests : IDisposable
{
    private readonly string _tmp;
    private readonly string? _savedEnv;

    public SessionIdentityTests()
    {
        _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_sess_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tmp);
        _savedEnv = Environment.GetEnvironmentVariable(SessionResolver.EnvironmentVariable);
        Environment.SetEnvironmentVariable(SessionResolver.EnvironmentVariable, null);
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(SessionResolver.EnvironmentVariable, _savedEnv);
        if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
    }

    [Fact]
    public void Explicit_Flag_Wins_Over_Environment()
    {
        Environment.SetEnvironmentVariable(SessionResolver.EnvironmentVariable, "from-env");

        var id = SessionResolver.Resolve("from-flag", _tmp);

        id.Name.Should().Be("from-flag");
        id.Origin.Should().Be(SessionOrigin.Explicit);
    }

    [Fact]
    public void Environment_Wins_Over_An_Ancestor_Store()
    {
        Directory.CreateDirectory(Path.Combine(_tmp, SessionResolver.DirectoryName));
        Environment.SetEnvironmentVariable(SessionResolver.EnvironmentVariable, "from-env");

        var id = SessionResolver.Resolve(null, _tmp);

        id.Name.Should().Be("from-env");
        id.Origin.Should().Be(SessionOrigin.Environment);
    }

    [Fact]
    public void Finds_The_Nearest_Ancestor_Store()
    {
        var nested = Path.Combine(_tmp, "a", "b", "c");
        Directory.CreateDirectory(nested);
        var store = Path.Combine(_tmp, "a", SessionResolver.DirectoryName);
        Directory.CreateDirectory(store);

        var id = SessionResolver.Resolve(null, nested);

        id.Origin.Should().Be(SessionOrigin.Ancestor);
        id.RootPath.Should().Be(store, "anywhere in a project must reach the same store, as git does towards .git");
    }

    [Fact]
    public void The_Nearest_Ancestor_Wins_Over_A_Further_One()
    {
        var outer = Path.Combine(_tmp, SessionResolver.DirectoryName);
        var innerDir = Path.Combine(_tmp, "inner");
        var inner = Path.Combine(innerDir, SessionResolver.DirectoryName);
        Directory.CreateDirectory(outer);
        Directory.CreateDirectory(inner);

        SessionResolver.Resolve(null, innerDir).RootPath.Should().Be(inner);
    }

    [Fact]
    public void A_Writable_Directory_With_No_Ancestor_Uses_Itself()
    {
        var id = SessionResolver.Resolve(null, _tmp);

        id.Origin.Should().Be(SessionOrigin.WorkingDirectory);
        id.RootPath.Should().Be(Path.Combine(_tmp, SessionResolver.DirectoryName));
    }

    [Fact]
    public void An_Unwritable_Directory_Falls_Back_To_User_State()
    {
        var id = SessionResolver.Resolve(null, Path.Combine(_tmp, "does-not-exist"));

        id.Origin.Should().Be(SessionOrigin.UserState);
        id.RootPath.Should().StartWith(UserStatePaths.FallbackSessionsDirectory());
    }

    [Fact]
    public void Resolving_Creates_Nothing()
    {
        SessionResolver.Resolve("some-name", _tmp);

        Directory.EnumerateFileSystemEntries(_tmp).Should().BeEmpty(
            "an ordinary run must leave no trace; the store appears when something is materialised");
    }

    [Theory]
    [InlineData("../../etc/passwd", "etc-passwd")]
    [InlineData("a/b", "a-b")]
    [InlineData("...", "default")]
    [InlineData("ok_name-1.2", "ok_name-1.2")]
    public void A_Session_Name_Stays_One_Path_Component(string input, string expected)
        => SessionResolver.Sanitize(input).Should().Be(expected,
            "the name is user-supplied and becomes a directory, so separators and traversal must not survive");

    [Fact]
    public void Two_Different_Paths_Get_Different_Fallback_Stores()
    {
        var a = SessionResolver.PathHash("/tmp/project-a");
        var b = SessionResolver.PathHash("/tmp/project-b");

        a.Should().NotBe(b);
        a.Should().Be(SessionResolver.PathHash("/tmp/project-a"), "and the same path is stable across runs");
    }
}
