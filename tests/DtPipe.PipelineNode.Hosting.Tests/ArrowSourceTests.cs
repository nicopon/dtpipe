using System.IO.Pipes;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public sealed class ArrowSourceTests : IDisposable
{
    private static readonly Schema Numbers = new Schema.Builder().Field(f => f.Name("n").DataType(Int64Type.Default).Nullable(false)).Build();
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    // A Unix socket path is at most 103 characters on macOS, whatever the machine's temporary directory.
    private readonly string _directory = Directory.CreateTempSubdirectory("dtps").FullName;
    private readonly List<ArrowSocketServer> _servers = [];

    public void Dispose()
    {
        foreach (var server in _servers) server.Dispose();
        try { Directory.Delete(_directory, true); } catch (IOException) { }
    }

    private static RecordBatch Batch(long first, int count, Schema? schema = null)
    {
        var builder = new Int64Array.Builder();
        for (var i = 0; i < count; i++) builder.Append(first + i);
        return new RecordBatch(schema ?? Numbers, [builder.Build()], count);
    }

    private async Task<ArrowSocketServer> StartAsync(Func<ArrowSourceContext, CancellationToken, IAsyncEnumerable<RecordBatch>> read, string name = "s")
    {
        var server = new ArrowSocketServer(name, Path.Combine(_directory, name + ".sock"), Numbers, read, NullLogger.Instance);
        _servers.Add(server);
        await server.StartAsync(CancellationToken.None);
        return server;
    }

    private static async Task<NamedPipeClientStream> ConnectAsync(ArrowSocketServer server)
    {
        var client = new NamedPipeClientStream(".", server.Path, PipeDirection.In, PipeOptions.Asynchronous);
        await client.ConnectAsync((int)Wait.TotalMilliseconds);
        return client;
    }

    private static async Task<byte[]> ReadAllAsync(ArrowSocketServer server)
    {
        await using var client = await ConnectAsync(server);
        using var bytes = new MemoryStream();
        await client.CopyToAsync(bytes).WaitAsync(Wait);
        return bytes.ToArray();
    }

    private static readonly byte[] EndMarker = [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];

    private static bool EndsWithMarker(byte[] stream) => stream.AsSpan().EndsWith(EndMarker);

    private static async IAsyncEnumerable<RecordBatch> Counting(int batches, [EnumeratorCancellation] CancellationToken ct)
    {
        for (var i = 0; i < batches; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return Batch(i * 10, 10);
            await Task.Yield();
        }
    }

    [Fact]
    public void The_socket_exists_as_soon_as_the_server_is_built()
    {
        var server = new ArrowSocketServer("s", Path.Combine(_directory, "built.sock"), Numbers, (_, ct) => Counting(1, ct), NullLogger.Instance);
        _servers.Add(server);
        Assert.True(File.Exists(server.Path));
    }

    [Fact]
    public void The_sockets_folder_is_the_current_users_alone()
    {
        if (OperatingSystem.IsWindows()) return;
        // The folder is made loose first, as a shared temporary directory would be: the server takes the others' rights away.
        var folder = Directory.CreateDirectory(Path.Combine(_directory, "loose"), UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
                                                                                  | UnixFileMode.GroupRead | UnixFileMode.GroupExecute | UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
        File.SetUnixFileMode(folder.FullName, File.GetUnixFileMode(folder.FullName) | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
        var server = new ArrowSocketServer("s", Path.Combine(folder.FullName, "s.sock"), Numbers, (_, ct) => Counting(1, ct), NullLogger.Instance);
        _servers.Add(server);
        var others = UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute
                     | UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;
        Assert.Equal(UnixFileMode.None, File.GetUnixFileMode(folder.FullName) & others);
    }

    [Fact]
    public async Task The_schema_is_readable_while_the_producer_has_not_yielded()
    {
        var release = new TaskCompletionSource();
        var server = await StartAsync(Blocked);
        await using var client = await ConnectAsync(server);
        using var reader = new ArrowStreamReader(client);
        var schema = await Task.Run(() => reader.Schema).WaitAsync(Wait);
        Assert.Equal("n", Assert.Single(schema.FieldsList).Name);
        release.SetResult();

        async IAsyncEnumerable<RecordBatch> Blocked(ArrowSourceContext context, [EnumeratorCancellation] CancellationToken ct)
        {
            await release.Task.WaitAsync(ct);
            yield return Batch(0, 1);
        }
    }

    [Fact]
    public async Task A_finished_producer_ends_the_stream_with_its_marker_and_every_row()
    {
        var server = await StartAsync((_, ct) => Counting(3, ct));
        var bytes = await ReadAllAsync(server);
        Assert.True(EndsWithMarker(bytes));

        using var reader = new ArrowStreamReader(new MemoryStream(bytes));
        var rows = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch) rows += batch.Length;
        Assert.Equal(30, rows);
    }

    [Fact]
    public async Task A_producer_that_throws_leaves_no_end_marker()
    {
        var server = await StartAsync(Failing);
        var bytes = await ReadAllAsync(server);
        Assert.NotEmpty(bytes);
        Assert.False(EndsWithMarker(bytes));

        // The server keeps serving: the next connection is answered.
        Assert.False(EndsWithMarker(await ReadAllAsync(server)));

        static async IAsyncEnumerable<RecordBatch> Failing(ArrowSourceContext context, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return Batch(0, 5);
            await Task.Yield();
            throw new TimeoutException("the external API did not answer");
        }
    }

    [Fact]
    public async Task A_producer_cancelled_by_its_own_timeout_is_a_failure_not_a_stop()
    {
        var server = await StartAsync(TimedOut);
        Assert.False(EndsWithMarker(await ReadAllAsync(server)));

        static async IAsyncEnumerable<RecordBatch> TimedOut(ArrowSourceContext context, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return Batch(0, 5);
            await Task.Yield();
            throw new TaskCanceledException("an HttpClient timeout");
        }
    }

    [Fact]
    public async Task A_batch_of_another_schema_ends_the_stream_without_its_marker()
    {
        var floats = new Schema.Builder().Field(f => f.Name("n").DataType(FloatType.Default).Nullable(false)).Build();
        var server = await StartAsync(Wrong);
        Assert.False(EndsWithMarker(await ReadAllAsync(server)));

        async IAsyncEnumerable<RecordBatch> Wrong(ArrowSourceContext context, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            yield return new RecordBatch(floats, [new FloatArray.Builder().Append(1.5f).Build()], 1);
        }
    }

    [Fact]
    public async Task A_batch_of_an_equal_schema_built_apart_is_accepted()
    {
        var same = new Schema.Builder().Field(f => f.Name("n").DataType(new Int64Type()).Nullable(false)).Build();
        var server = await StartAsync((_, ct) => Own(same, ct));
        Assert.True(EndsWithMarker(await ReadAllAsync(server)));

        static async IAsyncEnumerable<RecordBatch> Own(Schema schema, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            yield return Batch(0, 2, schema);
        }
    }

    [Fact]
    public async Task A_reader_that_leaves_stops_the_producer()
    {
        var stopped = new TaskCompletionSource();
        var server = await StartAsync(Endless);
        var client = await ConnectAsync(server);
        using (var reader = new ArrowStreamReader(client))
            Assert.NotNull(await reader.ReadNextRecordBatchAsync());
        await client.DisposeAsync();

        await stopped.Task.WaitAsync(Wait);

        async IAsyncEnumerable<RecordBatch> Endless(ArrowSourceContext context, [EnumeratorCancellation] CancellationToken ct)
        {
            try
            {
                for (long i = 0; ; i += 1000)
                {
                    yield return Batch(i, 1000);
                    await Task.Yield();
                }
            }
            finally
            {
                stopped.TrySetResult();
            }
        }
    }

    [Fact]
    public async Task Each_connection_runs_the_producer_once_and_is_numbered()
    {
        var seen = new List<int>();
        var server = await StartAsync((context, ct) =>
        {
            lock (seen) seen.Add(context.Connection);
            return Counting(1, ct);
        });
        await Task.WhenAll(ReadAllAsync(server), ReadAllAsync(server), ReadAllAsync(server));
        lock (seen) Assert.Equal([1, 2, 3], seen.Order());
    }

    [Fact]
    public void A_source_is_declared_as_a_brick_and_a_variable_of_the_node()
    {
        var services = new ServiceCollection();
        services.AddDtPipeNode(_ => { }).AddArrowSource("orders", "Orders", Numbers, (_, ct) => Counting(1, ct), "The shop's orders", _directory);

        var definition = services.BuildServiceProvider().GetRequiredService<NodeDefinition>();
        var brick = Assert.Single(definition.Bricks);
        Assert.Equal(("orders", BrickKind.Source), (brick.Id, brick.Kind));
        var variable = Assert.Single(definition.Variables);
        Assert.Equal("DTPIPE_SRC_ORDERS", variable.Name);
        // The brick holds the variable's name, never the path: the path differs by process and the brick's text is its version.
        var text = JobYaml.Serialize(brick.Branch);
        Assert.Contains("${{DTPIPE_SRC_ORDERS}}", text);
        Assert.DoesNotContain(_directory, text);
        Assert.StartsWith(_directory, variable.Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData("1orders")]
    [InlineData("or ders")]
    [InlineData("../orders")]
    public void A_source_id_that_is_no_word_is_refused(string id)
    {
        var node = new ServiceCollection().AddDtPipeNode(_ => { });
        Assert.Throws<ArgumentException>(() => node.AddArrowSource(id, "t", Numbers, (_, ct) => Counting(1, ct), socketDirectory: _directory));
    }

    [Fact]
    public void Two_ids_that_name_the_same_variable_are_refused()
    {
        var node = new ServiceCollection().AddDtPipeNode(_ => { });
        node.AddArrowSource("a-b", "t", Numbers, (_, ct) => Counting(1, ct), socketDirectory: _directory);
        Assert.Throws<InvalidOperationException>(() => node.AddArrowSource("a_b", "t", Numbers, (_, ct) => Counting(1, ct), socketDirectory: _directory));
    }

    [Fact]
    public void A_socket_path_too_long_for_a_unix_socket_is_refused_at_registration()
    {
        if (OperatingSystem.IsWindows()) return;
        var node = new ServiceCollection().AddDtPipeNode(_ => { });
        var directory = Path.Combine(_directory, new string('d', ArrowSourceExtensions.MaxSocketPathLength));
        Assert.Throws<ArgumentException>(() => node.AddArrowSource("orders", "t", Numbers, (_, ct) => Counting(1, ct), socketDirectory: directory));
    }

    [Fact]
    public async Task The_longest_accepted_socket_path_binds()
    {
        if (OperatingSystem.IsWindows()) return;
        var file = $"dtps-{Environment.ProcessId}-x.sock";
        var padding = ArrowSourceExtensions.MaxSocketPathLength - file.Length - 1 - _directory.Length - 1;
        var directory = Path.Combine(_directory, new string('d', padding));
        var node = new ServiceCollection().AddDtPipeNode(_ => { });
        node.AddArrowSource("x", "t", Numbers, (_, ct) => Counting(1, ct), socketDirectory: directory);
        node.Services.AddLogging();
        var provider = node.Services.BuildServiceProvider();
        var path = provider.GetRequiredService<NodeDefinition>().Variables.Single().Value;
        Assert.Equal(ArrowSourceExtensions.MaxSocketPathLength, path.Length);
        // The server alone: the node agent beside it needs a coordinator.
        var registration = node.Services.Last(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService));
        using var server = Assert.IsType<ArrowSocketServer>(registration.ImplementationFactory!(provider));
        Assert.True(File.Exists(path));
    }
}
