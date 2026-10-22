using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>How the node keeps a cursor for its child: a state file of its own on each sink branch that sets one, read once the child has exited.</summary>
public sealed class FragmentCursorsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "cursors-" + Guid.NewGuid().ToString("N"));

    public FragmentCursorsTests() => Directory.CreateDirectory(_dir);

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private const string Job = """
        src:
          input: sqlite:a
          output: arrow:-
        snk:
          from: src
          output: sqlite:b
          cursor: id
        plain:
          from: src
          output: sqlite:c
        """;

    [Fact]
    public void Only_A_Branch_That_Sets_A_Cursor_Gets_A_State_File_Of_The_Nodes_Own()
    {
        var (yaml, files) = FragmentCursors.Arm(Job, _dir, "run-1-frag");

        var path = Assert.Single(files).Value;
        Assert.Equal(("snk", Path.Combine(_dir, "run-1-frag-snk.cursor.json")), (files.Keys.Single(), path));
        Assert.Contains($"state: {path}", yaml);
        Assert.Equal(1, yaml.Split("state:").Length - 1);
    }

    [Fact]
    public void A_Job_With_No_Cursor_Is_Left_As_It_Is()
    {
        var (yaml, files) = FragmentCursors.Arm("a:\n  input: x\n  output: y\n", _dir, "s");

        Assert.Empty(files);
        Assert.Equal("a:\n  input: x\n  output: y\n", yaml);
    }

    [Fact]
    public void The_Value_The_Child_Wrote_Is_Reported_And_A_Child_That_Saw_No_Row_Reports_Nothing()
    {
        var (_, files) = FragmentCursors.Arm(Job, _dir, "r");
        Assert.Null(FragmentCursors.Read(files, NullLogger.Instance));

        File.WriteAllText(files["snk"], """{"version":1,"cursor":{"column":"id","value":"42","type":"integer"},"last_run":{"status":"success"}}""");

        var report = Assert.Single(FragmentCursors.Read(files, NullLogger.Instance)!);
        Assert.Equal(("snk", new CursorReport("id", "integer", "42")), (report.Key, report.Value));
    }

    [Theory]
    [InlineData("""{"cursor":{"column":"id","value":"1'; --","type":"string"}}""")]
    [InlineData("""{"cursor":{"column":"id","value":"x","type":"blob"}}""")]
    [InlineData("not json")]
    public void A_Value_Out_Of_The_Rule_Or_A_File_That_Cannot_Be_Read_Is_Not_Reported(string content)
    {
        var (_, files) = FragmentCursors.Arm(Job, _dir, "r");
        File.WriteAllText(files["snk"], content);

        Assert.Null(FragmentCursors.Read(files, NullLogger.Instance));
    }
}
