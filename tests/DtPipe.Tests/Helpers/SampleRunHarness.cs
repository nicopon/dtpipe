using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using DtPipe.Services;
using DtPipe.Services.Pipeline;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace DtPipe.Tests.Helpers;

/// <summary>
/// Drives a sample run the way the engine does — real PipelineExecutor, real sink, real tap —
/// and assembles the report through the same SampleReportBuilder the engine calls.
///
/// It is a harness, not an analyser: it owns no transformation semantics of its own. That
/// distinction is the point of the whole exercise, so it must hold in the tests too.
/// </summary>
internal static class SampleRunHarness
{
    private static readonly PipelineExecutor Executor = new(
        [new DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory(NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance)],
        [new DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory()],
        NullLogger<PipelineExecutor>.Instance);

    public static async Task<SampleReport> AnalyzeAsync(
        IStreamReader reader,
        List<IDataTransformer> pipeline,
        int sampleCount,
        object? writer = null,
        CancellationToken ct = default)
    {
        await reader.OpenAsync(ct);

        var schema = reader.Columns ?? throw new InvalidOperationException("Reader columns must be initialized before analysis.");
        // sampleCount bounds the source below, through Limit; the tap only holds a ceiling.
        var tap = new SampleTapRecorder();
        tap.OnStageSchema(0, "reader", schema, reader is IColumnarStreamReader);

        for (var i = 0; i < pipeline.Count; i++)
        {
            schema = await pipeline[i].InitializeAsync(schema, ct);
            tap.OnStageSchema(i + 1, pipeline[i].GetType().Name.Replace("DataTransformer", ""), schema, isColumnar: false);
        }

        var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(pipeline);
        foreach (var s in segments)
        {
            s.InputSchema = reader.Columns!;
            s.OutputSchema = schema;
        }

        IDataWriter sink = writer is IDataWriter realWriter ? SampleModeSink.Wrap(realWriter) : new NoOpRowWriter();
        using var cts = new CancellationTokenSource();
        await Executor.ExecuteSegmentedPipelineAsync(
            reader, sink, segments, schema,
            new PipelineOptions { BatchSize = 1024, Limit = sampleCount, DryRunCount = sampleCount },
            Mock.Of<IExportProgress>(), cts, cts.Token, tap);

        TargetSchemaInfo? target = null;
        string? inspectionError = null;
        if (writer is ISchemaInspector inspector)
        {
            try { target = await inspector.InspectTargetAsync(ct); }
            catch (Exception ex) { inspectionError = ex.Message; }
        }

        var run = tap.Build(sampleCount, (sink as ISampleModeSink)?.RowsWritten ?? 0);

        return SampleReportBuilder.Build(
            run,
            pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", "")).ToList(),
            (writer as IHasSqlDialect)?.Dialect,
            writer as IKeyValidator,
            target,
            inspectionError,
            tap.TypeHints);
    }

    private sealed class NoOpRowWriter : IRowDataWriter
    {
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
