using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using System.IO.Pipes;
using AwesomeAssertions;
using DtPipe.Adapters.Arrow;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Arrow;

/// <summary>
/// <c>arrow:pipe://&lt;name&gt;</c>, the location form a pipeline node's own excess edge (J2) wires
/// a fragment's <c>--bind-input</c>/<c>--bind-output</c> onto. Client-only by design - every test
/// here plays the server itself, the role a real pipeline node always takes before launching the
/// child that opens this side.
/// </summary>
public class ArrowPipeTests
{
    private static readonly Schema Schema =
        new([new Field("n", Int32Type.Default, nullable: false)], null);

    private static string NewPipeName() => $"dtpipe-test-{Guid.NewGuid():N}"[..24];

    private static RecordBatch MakeBatch(int start, int count)
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < count; i++) builder.Append(start + i);
        return new RecordBatch(Schema, [builder.Build()], count);
    }

    [Fact]
    public async Task A_reader_round_trips_a_complete_stream_over_a_named_pipe()
    {
        var pipeName = NewPipeName();
        await using var server = new NamedPipeServerStream(pipeName, PipeDirection.Out, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

        var serverTask = Task.Run(async () =>
        {
            await server.WaitForConnectionAsync();
            using var writer = new ArrowStreamWriter(server, Schema, leaveOpen: true);
            using var batch = MakeBatch(0, 4);
            await writer.WriteRecordBatchAsync(batch);
            await writer.WriteEndAsync();
        });

        await using var reader = new ArrowAdapterStreamReader($"pipe://{pipeName}", new ArrowReaderOptions());
        await reader.OpenAsync();

        var rows = 0;
        await foreach (var batch in reader.ReadRecordBatchesAsync().WithCancellation(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token))
            using (batch) rows += batch.Length;

        await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
        rows.Should().Be(4);
    }

    [Fact]
    public async Task A_writer_round_trips_a_complete_stream_over_a_named_pipe()
    {
        var pipeName = NewPipeName();
        await using var server = new NamedPipeServerStream(pipeName, PipeDirection.In, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

        var receivedTask = Task.Run(async () =>
        {
            await server.WaitForConnectionAsync();
            using var reader = new global::Apache.Arrow.Ipc.ArrowStreamReader(server);
            var rows = 0;
            while (await reader.ReadNextRecordBatchAsync() is { } batch)
                using (batch) rows += batch.Length;
            return rows;
        });

        var writer = new ArrowAdapterDataWriter($"pipe://{pipeName}");
        await writer.InitializeAsync([new PipeColumnInfo("n", typeof(int), false)]).AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        await writer.WriteRecordBatchAsync(MakeBatch(0, 5));
        await writer.CompleteAsync();
        await writer.DisposeAsync();

        (await receivedTask.WaitAsync(TimeSpan.FromSeconds(10))).Should().Be(5);
    }

    /// <summary>
    /// The rupture invariant (<c>ArrowStreamTerminationProbe</c>) must hold over a pipe-backed stream
    /// exactly as it does over a file: a server that closes mid-message, without the IPC
    /// end-of-stream marker, must be refused rather than read as a complete, if short, transfer.
    /// </summary>
    [Fact]
    public async Task A_pipe_closed_without_the_end_of_stream_marker_is_refused()
    {
        var pipeName = NewPipeName();
        // Not `await using` at method scope: the reader must observe an actual close, not just a
        // pause in writing, so the server stream itself has to be disposed inside serverTask once
        // its truncated write is done - keeping it alive for the whole test would let the reader
        // wait forever for bytes that were merely delayed, not a peer that hung up.
        var server = new NamedPipeServerStream(pipeName, PipeDirection.Out, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

        var serverTask = Task.Run(async () =>
        {
            await server.WaitForConnectionAsync();
            using (var writer = new ArrowStreamWriter(server, Schema, leaveOpen: true))
            {
                using var batch = MakeBatch(0, 4);
                await writer.WriteRecordBatchAsync(batch);
                // Deliberately no WriteEndAsync(): the server disconnects instead.
            }
            await server.DisposeAsync();
        });

        await using var reader = new ArrowAdapterStreamReader($"pipe://{pipeName}", new ArrowReaderOptions());
        await reader.OpenAsync();

        var act = async () =>
        {
            await foreach (var batch in reader.ReadRecordBatchesAsync().WithCancellation(new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token))
                batch.Dispose();
        };

        await act.Should().ThrowAsync<EndOfStreamException>()
            .Where(ex => ex.Message.Contains("truncated"));

        await serverTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task InspectTargetAsync_NeverOpensAFileForAPipeLocation()
    {
        // No server is ever created for this pipe name: if InspectTargetAsync tried
        // File.OpenRead on it, the call would hang or throw FileNotFoundException instead of
        // returning promptly with "exists, unknown".
        var writer = new ArrowAdapterDataWriter($"pipe://{NewPipeName()}");

        var info = await writer.InspectTargetAsync().WaitAsync(TimeSpan.FromSeconds(1));

        info.Should().NotBeNull();
        info!.Exists.Should().BeTrue();
        info.Columns.Should().BeEmpty();
    }

    [Fact]
    public async Task AConnectWithNoServer_TimesOutNamingThePipe()
    {
        var pipeName = NewPipeName();
        await using var reader = new ArrowAdapterStreamReader($"pipe://{pipeName}", new ArrowReaderOptions());

        var act = async () => await reader.OpenAsync();

        var ex = await act.Should().ThrowAsync<TimeoutException>().WaitAsync(TimeSpan.FromSeconds(15));
        ex.Which.Message.Should().Contain(pipeName);
    }
}
