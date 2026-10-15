using System.Runtime.Versioning;
using DtPipe.PipelineNode.Hosting;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>A node announces what its dtpipe binary says of itself, and nothing when it says nothing usable.</summary>
public sealed class DtPipeVersionTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("dtpipe-version-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    [UnsupportedOSPlatform("windows")]
    private DtPipeCommand Binary(string script)
    {
        var path = Path.Combine(_dir, "dtpipe");
        File.WriteAllText(path, "#!/bin/sh\n" + script + "\n");
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        return new DtPipeCommand(path, _dir, new Dictionary<string, string>());
    }

    [Fact]
    public async Task The_Version_Is_What_The_Binary_Prints_After_Its_Name()
    {
        if (OperatingSystem.IsWindows()) return;
        Assert.Equal("1.9.0+abc123", await Binary("echo 'dtpipe 1.9.0+abc123'").VersionAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("echo 'dtpipe 1.9.0'; exit 1")]
    [InlineData("echo 'something else'")]
    [InlineData("echo 'dtpipe '")]
    public async Task A_Binary_That_Fails_Or_Says_Anything_Else_Gives_No_Version(string script)
    {
        if (OperatingSystem.IsWindows()) return;
        Assert.Null(await Binary(script).VersionAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task A_Binary_That_Does_Not_Start_Gives_No_Version()
    {
        var missing = new DtPipeCommand(Path.Combine(_dir, "missing"), _dir, new Dictionary<string, string>());
        Assert.Null(await missing.VersionAsync(TestContext.Current.CancellationToken));
    }
}
