using System.Diagnostics;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A source a host serves on an Arrow socket, read by the real <c>dtpipe</c> binary as any reader would: each case is judged
/// on the exit code and on the rows that came out, never on the exit code alone, since a run that fails to see the rows can
/// still exit 0. <c>ArrowSourceTests</c> (Hosting.Tests) holds the same socket against an in-process reader, byte by byte.
/// </summary>
public sealed class ArrowSourceReadTests : IDisposable
{
    private const int Batches = 12;
    private const int RowsPerBatch = 100;
    private const int AllRows = Batches * RowsPerBatch;
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static readonly Schema Numbers = new Schema.Builder()
        .Field(f => f.Name("n").DataType(Int64Type.Default).Nullable(false))
        .Build();

    // A Unix socket path is at most 103 characters on macOS, whatever the machine's temporary directory.
    private readonly string _directory = Directory.CreateTempSubdirectory("dtpr").FullName;
    private readonly List<ArrowSocketServer> _servers = [];

    public void Dispose()
    {
        foreach (var server in _servers) server.Dispose();
        try { Directory.Delete(_directory, true); } catch (IOException) { }
    }

    private async Task<ArrowSocketServer> ServeAsync(
        Func<ArrowSourceContext, CancellationToken, IAsyncEnumerable<RecordBatch>> read, string name = "s")
    {
        var server = new ArrowSocketServer(name, Path.Combine(_directory, name + ".sock"), Numbers, read, NullLogger.Instance);
        _servers.Add(server);
        await server.StartAsync(CancellationToken.None);
        return server;
    }

    /// <summary>The numbers in pages, a pause after each so a reader that leaves is seen leaving before the last one.</summary>
    private static async IAsyncEnumerable<RecordBatch> Pages(
        int failAfter, Action? onPage, [EnumeratorCancellation] CancellationToken ct)
    {
        for (var page = 0; page < Batches; page++)
        {
            if (page == failAfter) throw new HttpRequestException("the source answered 503");
            await Task.Delay(20, ct);
            onPage?.Invoke();
            var numbers = new Int64Array.Builder();
            for (var i = 0; i < RowsPerBatch; i++) numbers.Append(page * RowsPerBatch + i);
            yield return new RecordBatch(Numbers, [numbers.Build()], RowsPerBatch);
        }
    }

    private sealed record Result(int Code, long Rows, string Stderr);

    private static async Task<Result> ReadAsync(string socket, params string[] extra)
    {
        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var arg in new[] { "-i", "arrow:pipe://" + socket, "-o", "csv:-", "--no-stats" }.Concat(extra))
            psi.ArgumentList.Add(arg);
        psi.Environment["NO_COLOR"] = "1";
        psi.Environment["DTPIPE_NO_TUI"] = "1";

        using var process = Process.Start(psi)!;
        using var timeout = new CancellationTokenSource(Limit);
        try
        {
            var stderr = process.StandardError.ReadToEndAsync(timeout.Token);
            long lines = 0;
            while (await process.StandardOutput.ReadLineAsync(timeout.Token) is { } line)
                if (line.Length > 0) lines++;
            await process.WaitForExitAsync(timeout.Token);
            // The first line is the CSV header.
            return new Result(process.ExitCode, Math.Max(0, lines - 1), await stderr);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"dtpipe did not end within {Limit.TotalSeconds:F0}s reading {socket}.");
        }
    }

    [Fact]
    public async Task A_full_read_yields_every_row()
    {
        var server = await ServeAsync((_, ct) => Pages(int.MaxValue, null, ct));

        var result = await ReadAsync(server.Path);

        Assert.True(result is { Code: 0, Rows: AllRows }, $"exit {result.Code}, rows {result.Rows}/{AllRows}: {result.Stderr}");
    }

    [Fact]
    public async Task A_reader_that_stops_early_stops_the_producer()
    {
        var produced = 0;
        var stopped = new TaskCompletionSource();
        var server = await ServeAsync((_, ct) => Stopping(ct));

        var result = await ReadAsync(server.Path, "--limit", "5");
        await stopped.Task.WaitAsync(Limit);

        Assert.True(result is { Code: 0, Rows: 5 }, $"exit {result.Code}, rows {result.Rows}/5: {result.Stderr}");
        Assert.True(Volatile.Read(ref produced) < Batches, $"{produced}/{Batches} pages were produced after the reader left");

        async IAsyncEnumerable<RecordBatch> Stopping([EnumeratorCancellation] CancellationToken ct)
        {
            try
            {
                await foreach (var batch in Pages(int.MaxValue, () => Interlocked.Increment(ref produced), ct)) yield return batch;
            }
            finally
            {
                stopped.TrySetResult();
            }
        }
    }

    [Fact]
    public async Task A_producer_that_fails_fails_the_run_and_the_next_read_is_whole()
    {
        var server = await ServeAsync((context, ct) => Pages(context.Connection == 1 ? 3 : int.MaxValue, null, ct));

        var failed = await ReadAsync(server.Path);
        var again = await ReadAsync(server.Path);

        Assert.True(failed.Code != 0 && failed.Stderr.Contains("truncated", StringComparison.OrdinalIgnoreCase),
            $"exit {failed.Code}, rows {failed.Rows}: {failed.Stderr}");
        Assert.True(again is { Code: 0, Rows: AllRows }, $"exit {again.Code}, rows {again.Rows}/{AllRows}: {again.Stderr}");
    }

    [Fact]
    public async Task Concurrent_reads_each_get_every_row()
    {
        var server = await ServeAsync((_, ct) => Pages(int.MaxValue, null, ct));

        var results = await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => ReadAsync(server.Path)));

        Assert.All(results, r => Assert.True(r is { Code: 0, Rows: AllRows }, $"exit {r.Code}, rows {r.Rows}/{AllRows}: {r.Stderr}"));
    }

    [Fact]
    public async Task A_socket_nobody_listens_on_fails_the_run()
    {
        var result = await ReadAsync(Path.Combine(_directory, "nobody.sock"));

        Assert.NotEqual(0, result.Code);
    }
}
