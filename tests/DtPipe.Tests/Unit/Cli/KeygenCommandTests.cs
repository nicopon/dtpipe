using DtPipe.Cli.Commands;
using DtPipe.Jobs.Coordinator;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

[Collection("console-serial")]
public sealed class KeygenCommandTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("dtpipe-key-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static async Task<(string Stdout, string Stderr, int Exit)> RunAsync(params string[] args)
    {
        var (previousOut, previousError) = (Console.Out, Console.Error);
        var (stdout, stderr) = (new StringWriter(), new StringWriter());
        Console.SetOut(stdout);
        Console.SetError(stderr);
        try
        {
            var exit = await new KeygenCommand().Parse(args).InvokeAsync();
            return (stdout.ToString(), stderr.ToString(), exit);
        }
        finally
        {
            Console.SetOut(previousOut);
            Console.SetError(previousError);
        }
    }

    [Fact]
    public async Task A_New_Key_Is_Printed_Alone_In_The_Shape_The_Coordinator_Reads_And_Differs_Each_Time()
    {
        var (first, _, exit) = await RunAsync("runner-1");
        var (second, _, _) = await RunAsync("runner-1");

        Assert.Equal(0, exit);
        Assert.Equal("runner-1", CoordinatorApi.ClientIdOfKey(first.TrimEnd('\n', '\r')));
        Assert.NotEqual(first, second);
    }

    [Theory]
    [InlineData("Runner")]
    [InlineData("a_b")]
    [InlineData("-a")]
    public async Task A_Client_Id_The_Coordinator_Would_Not_Read_Back_Is_Refused_And_Prints_No_Key(string id)
    {
        var (stdout, stderr, exit) = await RunAsync(id);

        Assert.Equal(1, exit);
        Assert.Empty(stdout);
        Assert.Contains("is not a client id", stderr);
    }

    [Fact]
    public async Task A_Key_Written_To_A_File_Is_Private_Kept_Out_Of_The_Output_And_Never_Replaced()
    {
        var path = Path.Combine(_dir, "admin.key");

        var (stdout, stderr, exit) = await RunAsync("admin", "--out", path);

        Assert.Equal(0, exit);
        var key = File.ReadAllText(path);
        Assert.Equal("admin", CoordinatorApi.ClientIdOfKey(key));
        Assert.DoesNotContain(key, stdout + stderr);
        if (!OperatingSystem.IsWindows())
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));

        var (_, refusal, again) = await RunAsync("admin", "--out", path);

        Assert.Equal(1, again);
        Assert.Contains("never replaced", refusal);
        Assert.Equal(key, File.ReadAllText(path));
    }
}
