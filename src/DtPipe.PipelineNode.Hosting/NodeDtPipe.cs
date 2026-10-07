namespace DtPipe.PipelineNode.Hosting;

/// <summary>What a dtpipe command a handler ran left behind.</summary>
public sealed record NodeDtPipeResult(int ExitCode, string Stdout, string Stderr)
{
    public bool Succeeded => ExitCode == 0;

    /// <summary>The last non-empty stderr lines: where dtpipe states why it refused.</summary>
    public string Diagnostic => string.Join('\n',
        Stderr.Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).TakeLast(4));
}

/// <summary>
/// Runs the node's dtpipe the way its own children run: with the node's variables in the environment, so
/// a command names a database through <c>${{VARIABLE}}</c> and its value never enters an argument.
/// </summary>
public sealed class NodeDtPipe
{
    private readonly DtPipeCommand _command;

    internal NodeDtPipe(DtPipeCommand command) => _command = command;

    /// <summary>A directory of the node's own, for what a command writes.</summary>
    public string WorkingDirectory => _command.WorkingDirectory;

    public async Task<NodeDtPipeResult> RunAsync(IEnumerable<string> args, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        var result = await _command.RunAsync(args, ct, timeout);
        return new NodeDtPipeResult(result.ExitCode, result.Stdout, result.Stderr);
    }
}
