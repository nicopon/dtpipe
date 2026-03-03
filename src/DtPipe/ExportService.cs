using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
using DtPipe.Core.Resilience;
using DtPipe.Core.Validation;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Apache.Arrow;
using System.Runtime.CompilerServices;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe;

public class ExportService
{
	private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
	private readonly IEnumerable<IDataWriterFactory> _writerFactories;
	private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
	private readonly IEnumerable<IRowToColumnarBridgeFactory> _bridgeFactories;
	private readonly IEnumerable<IColumnarToRowBridgeFactory> _columnarToRowBridgeFactories;
	private readonly OptionsRegistry _optionsRegistry;
	private readonly IExportObserver _observer;
	private readonly IMemoryChannelRegistry? _channelRegistry;
	private readonly ILogger<ExportService> _logger;

	public ExportService(
		IEnumerable<IStreamReaderFactory> readerFactories,
		IEnumerable<IDataWriterFactory> writerFactories,
		IEnumerable<IDataTransformerFactory> transformerFactories,
		IEnumerable<IRowToColumnarBridgeFactory> bridgeFactories,
		IEnumerable<IColumnarToRowBridgeFactory> columnarToRowBridgeFactories,
		OptionsRegistry optionsRegistry,
		IExportObserver observer,
		ILogger<ExportService> logger,
		IMemoryChannelRegistry? channelRegistry = null)
	{
		_readerFactories = readerFactories;
		_writerFactories = writerFactories;
		_transformerFactories = transformerFactories;
		_bridgeFactories = bridgeFactories;
		_columnarToRowBridgeFactories = columnarToRowBridgeFactories;
		_optionsRegistry = optionsRegistry;
		_observer = observer;
		_logger = logger;
		_channelRegistry = channelRegistry;
	}

	public async Task RunExportAsync(
		PipelineOptions options,
		string providerName,
		string outputPath,
		CancellationToken ct,
		List<IDataTransformer> pipeline,
		IStreamReaderFactory readerFactory,
		IDataWriterFactory writerFactory,
		OptionsRegistry registry,
		string? alias = null)
	{
		if (_logger.IsEnabled(LogLevel.Information))
			_logger.LogInformation("Starting export from {Provider} to {OutputPath}", providerName, ConnectionStringSanitizer.Sanitize(outputPath));

		// Display Source Info
		_observer.ShowIntro(providerName, outputPath);

		_observer.ShowConnectionStatus(false, null);

		// Initialize RetryPolicy early to cover setup phase
		var retryPolicy = new RetryPolicy(options.MaxRetries, TimeSpan.FromMilliseconds(options.RetryDelayMs), _logger);

		await using var reader = readerFactory.Create(registry);
		await retryPolicy.ExecuteAsync(() => reader.OpenAsync(ct), ct);

		_observer.ShowConnectionStatus(true, reader.Columns?.Count);

		if (reader.Columns is null || reader.Columns.Count == 0)
		{
			_observer.LogWarning("No columns returned by query.");
			return;
		}

		// Initialize pipeline to define Target Schema
		var currentSchema = reader.Columns ?? System.Array.Empty<PipeColumnInfo>();
		var transformerSchemas = new Dictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)>();

		if (pipeline.Count > 0)
		{
			var transformerNames = pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
			_observer.ShowPipeline(transformerNames);

			foreach (var t in pipeline)
			{
				var inputSchema = currentSchema;
				currentSchema = await t.InitializeAsync(currentSchema, ct);
				transformerSchemas[t] = (inputSchema, currentSchema);
			}
		}

		// Now that transformers are initialized, we can segment correctly based on IsColumnar
		var segments = GetPipelineSegments(pipeline);
		if (pipeline.Count > 0)
		{
			// Fill segment schemas for bridging
			foreach (var segment in segments)
			{
				segment.InputSchema = transformerSchemas.Count > 0 && segment.Transformers.Count > 0
					? transformerSchemas[segment.Transformers[0]].In
					: reader.Columns ?? System.Array.Empty<PipeColumnInfo>();

				segment.OutputSchema = transformerSchemas.Count > 0 && segment.Transformers.Count > 0
					? transformerSchemas[segment.Transformers[^1]].Out
					: currentSchema;
			}
		}

		// Update DAG registry if we are a branch
		if (!string.IsNullOrEmpty(alias) && _channelRegistry != null)
		{
			_channelRegistry.UpdateChannelColumns(alias, currentSchema ?? System.Array.Empty<PipeColumnInfo>());
		}

		// Dry-run mode
		if (options.DryRunCount > 0)
		{
			IDataWriter? writerForInspection = null;
			if (!string.IsNullOrEmpty(outputPath))
			{
				try
				{
					writerForInspection = writerFactory.Create(registry);
				}
				catch (Exception ex)
				{
					_observer.LogWarning($"Could not create writer for schema inspection during dry-run: {ex.Message}. Target schema compatibility will not be checked.");
				}
			}

			await _observer.RunDryRunAsync(reader, pipeline, options.DryRunCount, writerForInspection, transformerSchemas, ct);

			if (writerForInspection != null)
			{
				await writerForInspection.DisposeAsync();
			}
			return;
		}

		string writerName = writerFactory.ComponentName;
		_observer.ShowTarget(writerName, outputPath);

		var exportableSchema = currentSchema ?? throw new InvalidOperationException("Exportable schema is null.");
		await using var writer = writerFactory.Create(registry);

		// Execute Pre-Hook
		await ExecuteHookAsync(writer, "Pre-Hook", options.PreExec, ct);

		await retryPolicy.ExecuteValueAsync(() => writer.InitializeAsync(exportableSchema, ct), ct);

		// Schema Validation
		await retryPolicy.ExecuteAsync(() => ValidateAndMigrateSchemaAsync(writer, exportableSchema, options, ct), ct);

		// Use Observer to create Progress
		var transformerNamesList = pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
		using var progress = _observer.CreateProgressReporter(!options.NoStats, transformerNamesList);

		using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var effectiveCt = linkedCts.Token;

		try
		{
			var startTime = DateTime.UtcNow;

			// Execute Unified Pipeline
			await ExecuteSegmentedPipelineAsync(reader, writer, segments, exportableSchema, options, progress, retryPolicy, linkedCts, effectiveCt);

			await writer.CompleteAsync(ct);
			progress.Complete();

			var elapsed = DateTime.UtcNow - startTime;
			var rowsPerSecond = elapsed.TotalSeconds > 0 ? progress.GetMetrics().ReadCount / elapsed.TotalSeconds : 0;
			if (_logger.IsEnabled(LogLevel.Information))
				_logger.LogInformation("Export completed in {Elapsed}. Written {Rows} rows ({Speed:F1} rows/s).", elapsed, progress.GetMetrics().WriteCount, rowsPerSecond);

			// --- POST-EXEC HOOK ---
			await ExecuteHookAsync(writer, "Post-Hook", options.PostExec, ct);

			_observer.LogMessage($"[green]✓ Export completed successfully.[/]");

			await SaveMetricsAsync(progress, options.MetricsPath, ct);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Export failed");
			_observer.LogError(ex);

			// --- ON-ERROR HOOK ---
			try
			{
				await ExecuteHookAsync(writer, "On-Error Hook", options.OnErrorExec, CancellationToken.None, TimeSpan.FromSeconds(30));
			}
			catch (Exception hookEx)
			{
				_logger.LogError(hookEx, "On-Error Hook failed");
				_observer.LogError(hookEx);
			}

			throw;
		}
		finally
		{
			// --- FINALLY HOOK ---
			try
			{
				await ExecuteHookAsync(writer, "Finally Hook", options.FinallyExec, CancellationToken.None, TimeSpan.FromSeconds(30));
			}
			catch (Exception hookEx)
			{
				_logger.LogError(hookEx, "Finally Hook failed");
				_observer.LogError(hookEx);
			}
		}
	}

	/// <summary>
	/// Validates schema compatibility and performs auto-migration if enabled.
	/// </summary>
	private async Task ValidateAndMigrateSchemaAsync(
		IDataWriter writer,
		IReadOnlyList<PipeColumnInfo> exportableSchema,
		PipelineOptions options,
		CancellationToken ct)
	{
		if (options.NoSchemaValidation || writer is not ISchemaInspector inspector) return;

		_observer.LogMessage("Verifying target schema compatibility...");
		var targetSchema = await inspector.InspectTargetAsync(ct);
		var dialect = (writer as IHasSqlDialect)?.Dialect;
		var report = SchemaCompatibilityAnalyzer.Analyze(exportableSchema, targetSchema, dialect);

		foreach (var warning in report.Warnings) _observer.LogWarning(warning);
		foreach (var error in report.Errors) _observer.LogError(new Exception(error));

		// Auto-migrate if needed
		var missingCount = report.Columns.Count(c => c.Status == CompatibilityStatus.MissingInTarget);
		if (missingCount > 0 && options.AutoMigrate && writer is ISchemaMigrator migrator)
		{
			_observer.LogMessage($"[yellow]Auto-migrating schema: Adding {missingCount} missing columns...[/]");
			await migrator.MigrateSchemaAsync(report, ct);

			// Re-validate after migration
			targetSchema = await inspector.InspectTargetAsync(ct);
			report = SchemaCompatibilityAnalyzer.Analyze(exportableSchema, targetSchema, dialect);

			if (!report.IsCompatible && options.StrictSchema)
				throw new InvalidOperationException("Export aborted: Schema migration failed to resolve all incompatibilities in Strict Mode.");

			_observer.LogMessage("[green]Schema migration successful. Continuing export.[/]");
		}
		else if (!report.IsCompatible && options.StrictSchema)
		{
			throw new InvalidOperationException("Export aborted due to schema incompatibilities (Strict Mode).");
		}
	}

	private async Task ExecuteHookAsync(IDataWriter writer, string hookName, string? command, CancellationToken ct, TimeSpan? timeout = null)
	{
		if (string.IsNullOrWhiteSpace(command)) return;

		_observer.OnHookExecuting(hookName, command);

		if (timeout.HasValue)
		{
			using var hookCts = new CancellationTokenSource(timeout.Value);
			await writer.ExecuteCommandAsync(command, hookCts.Token);
		}
		else
		{
			await writer.ExecuteCommandAsync(command, ct);
		}
	}

	private async Task SaveMetricsAsync(IExportProgress progress, string? metricsPath, CancellationToken ct)
	{
		if (string.IsNullOrEmpty(metricsPath)) return;

		var metrics = progress.GetMetrics();
		try
		{
			var json = System.Text.Json.JsonSerializer.Serialize(metrics,
				new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
			await File.WriteAllTextAsync(metricsPath, json, ct);
			_observer.LogMessage($"   [grey]Metrics saved to: {metricsPath}[/]");
		}
		catch (Exception ex)
		{
			_observer.LogWarning($"Failed to save metrics: {ex.Message}");
		}
	}


    private static async Task DirectColumnarTransferAsync(
        IColumnarStreamReader reader,
        IColumnarDataWriter writer,
        int limit,
        IExportProgress progress,
        CancellationToken ct)
    {
        long rowCount = 0;
        await foreach (var batch in reader.ReadRecordBatchesAsync(ct))
        {
            if (writer.PrefersOwnershipTransfer)
            {
                var batchToWriter = batch;
                await writer.WriteRecordBatchAsync(batchToWriter, ct);
                progress.ReportRead(batchToWriter.Length);
                progress.ReportWrite(batchToWriter.Length);
                rowCount += batchToWriter.Length;
            }
            else
            {
                using (batch) // Ensure Arrow C release callback is called promptly
                {
                    var batchToWriter = batch;
                    if (limit > 0 && rowCount + batch.Length > limit)
                    {
                        var takeCount = limit - (int)rowCount;
                        // Note: Slicing would be better here if supported.
                    }

                    await writer.WriteRecordBatchAsync(batchToWriter, ct);
                    progress.ReportRead(batchToWriter.Length);
                    progress.ReportWrite(batchToWriter.Length);
                    rowCount += batchToWriter.Length;

                    if (limit > 0 && rowCount >= limit) break;
                }
            }
        }
    }

    private static void LogMemoryUsage(ILogger logger)
    {
        if (!logger.IsEnabled(LogLevel.Debug)) return;

        var managedMemory = GC.GetTotalMemory(false) / 1024 / 1024;
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        var totalMemory = process.WorkingSet64 / 1024 / 1024;

        logger.LogDebug("Memory Stats: Managed={Managed}MB, WorkingSet={Total}MB", managedMemory, totalMemory);
    }

    /// <summary>
    /// Helper to group individual rows into batches before sending to a channel.
    /// </summary>
    private class BatchChannelWriter : IAsyncDisposable
    {
        private readonly ChannelWriter<object?[][]> _target;
        private readonly int _batchSize;
        private readonly List<object?[]> _buffer;
        private readonly CancellationToken _ct;

        public BatchChannelWriter(ChannelWriter<object?[][]> target, int batchSize, CancellationToken ct)
        {
            _target = target;
            _batchSize = batchSize;
            _buffer = new List<object?[]>(batchSize);
            _ct = ct;
        }

        public async ValueTask WriteAsync(object?[] row)
        {
            _buffer.Add(row);
            if (_buffer.Count >= _batchSize)
            {
                await FlushAsync();
            }
        }

        public async ValueTask FlushAsync()
        {
            if (_buffer.Count == 0) return;
            // Write a copy of the buffer as an array to the channel
            await _target.WriteAsync(_buffer.ToArray(), _ct);
            _buffer.Clear();
        }

        public async ValueTask DisposeAsync()
        {
            await FlushAsync();
        }
    }

    private class PipelineSegment
    {
        public bool IsColumnar { get; }
        public List<IDataTransformer> Transformers { get; }
        public IReadOnlyList<PipeColumnInfo> InputSchema { get; set; } = System.Array.Empty<PipeColumnInfo>();
        public IReadOnlyList<PipeColumnInfo> OutputSchema { get; set; } = System.Array.Empty<PipeColumnInfo>();

        public PipelineSegment(bool isColumnar, List<IDataTransformer> transformers)
        {
            IsColumnar = isColumnar;
            Transformers = transformers;
        }
    }

    private List<PipelineSegment> GetPipelineSegments(List<IDataTransformer> pipeline)
    {
        var segments = new List<PipelineSegment>();
        if (pipeline.Count == 0) return segments;

        PipelineSegment? current = null;
        foreach (var t in pipeline)
        {
            bool isCol = t is IColumnarTransformer ct && ct.CanProcessColumnar;
            if (current == null || current.IsColumnar != isCol)
            {
                current = new PipelineSegment(isCol, new List<IDataTransformer>());
                segments.Add(current);
            }
            current.Transformers.Add(t);
        }
        return segments;
    }

    private async Task ExecuteSegmentedPipelineAsync(
        IStreamReader reader,
        IDataWriter writer,
        List<PipelineSegment> segments,
        IReadOnlyList<PipeColumnInfo> columns,
        PipelineOptions options,
        IExportProgress progress,
        RetryPolicy retryPolicy,
        CancellationTokenSource linkedCts,
        CancellationToken ct)
    {
        if (segments.Count == 0)
        {
            if (reader is IColumnarStreamReader cr && writer is IColumnarDataWriter cw)
                await DirectColumnarTransferAsync(cr, cw, options.Limit, progress, ct);
            else if (reader is not IColumnarStreamReader && writer is not IColumnarDataWriter)
                await DirectRowTransferAsync(reader, writer, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, retryPolicy, ct);
            else
            {
                // Mismatch between Reader and Writer with no transformers
                // Create a dummy segment to force the bridging logic below
                segments.Add(new PipelineSegment(writer is IColumnarDataWriter, new List<IDataTransformer>())
                {
                    InputSchema = columns,
                    OutputSchema = columns
                });
            }
        }

        if (segments.Count > 0)
        {

        IAsyncEnumerable<RecordBatch> currentColumnarSource = null!;
        IAsyncEnumerable<object?[]> currentRowSource = null!;
        bool isCurrentColumnar = false;

        if (reader is IColumnarStreamReader columnarReader)
        {
            currentColumnarSource = columnarReader.ReadRecordBatchesAsync(ct);
            isCurrentColumnar = true;
        }
        else
        {
            currentRowSource = ProduceRowStreamAsync(reader, options.Limit, options.SamplingRate, options.SamplingSeed, progress, ct);
            isCurrentColumnar = false;
        }

        foreach (var segment in segments)
        {
            if (segment.IsColumnar)
            {
                if (!isCurrentColumnar)
                {
                    var bridgeFac = _bridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No RowToColumnarBridgeFactory");
                    currentColumnarSource = BridgeRowsToColumnarAsync(currentRowSource, bridgeFac, segment.InputSchema, options.BatchSize, ct);
                    isCurrentColumnar = true;
                }
                currentColumnarSource = ApplyColumnarSegmentAsync(currentColumnarSource, segment.Transformers, progress, ct);
            }
            else
            {
                if (isCurrentColumnar)
                {
                    var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                    currentRowSource = BridgeColumnarToRowsAsync(currentColumnarSource, bridgeFac, ct);
                    isCurrentColumnar = false;
                }
                currentRowSource = ApplyRowSegmentAsync(currentRowSource, segment.Transformers, progress, ct);
            }
        }

        if (writer is IColumnarDataWriter columnarWriter)
        {
            if (!isCurrentColumnar)
            {
                var bridgeFac = _bridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No RowToColumnarBridgeFactory");
                currentColumnarSource = BridgeRowsToColumnarAsync(currentRowSource, bridgeFac, columns, options.BatchSize, ct);
            }
            await ConsumeColumnarStreamAsync(currentColumnarSource, columnarWriter, progress, retryPolicy, ct);
        }
        else
        {
            if (isCurrentColumnar)
            {
                var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                currentRowSource = BridgeColumnarToRowsAsync(currentColumnarSource, bridgeFac, ct);
            }
            await ConsumeRowStreamAsync(currentRowSource, writer, options.BatchSize, progress, retryPolicy, ct);
        }
    }
    }

    private async IAsyncEnumerable<object?[]> ProduceRowStreamAsync(IStreamReader reader, int limit, double samplingRate, int? samplingSeed, IExportProgress progress, [EnumeratorCancellation] CancellationToken ct)
    {
        Random? sampler = samplingRate > 0 && samplingRate < 1.0 ? (samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared) : null;
        long rowCount = 0;
        await foreach (var batch in reader.ReadBatchesAsync(1000, ct))
        {
            for (int i = 0; i < batch.Length; i++)
            {
                if (sampler != null && sampler.NextDouble() > samplingRate) continue;
                yield return batch.Span[i];
                progress.ReportRead(1);
                if (limit > 0 && ++rowCount >= limit) yield break;
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> BridgeRowsToColumnarAsync(IAsyncEnumerable<object?[]> rows, IRowToColumnarBridgeFactory factory, IReadOnlyList<PipeColumnInfo> columns, int batchSize, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var bridge = factory.CreateBridge();
        await bridge.InitializeAsync(columns, batchSize, ct);

        // Feed ingestion in background
        var ingestionTask = Task.Run(async () =>
        {
            try
            {
                var buffer = new List<object?[]>(batchSize);
                await foreach (var row in rows.WithCancellation(ct))
                {
                    buffer.Add(row);
                    if (buffer.Count >= batchSize)
                    {
                        await bridge.IngestRowsAsync(buffer.ToArray(), ct);
                        buffer.Clear();
                    }
                }
                if (buffer.Count > 0)
                {
                    await bridge.IngestRowsAsync(buffer.ToArray(), ct);
                }
                await bridge.CompleteAsync(ct);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during row ingestion in bridge");
                bridge.Fault(ex);
            }
        }, ct);

        // Stream batches as they arrive
        await foreach (var batch in bridge.ReadRecordBatchesAsync(ct))
        {
            yield return batch;
        }

        await ingestionTask;
    }

    private async IAsyncEnumerable<object?[]> BridgeColumnarToRowsAsync(IAsyncEnumerable<RecordBatch> batches, IColumnarToRowBridgeFactory factory, [EnumeratorCancellation] CancellationToken ct)
    {
        var bridge = factory.CreateBridge();
        await foreach (var batch in batches)
        {
            await foreach (var row in bridge.ConvertBatchToRowsAsync(batch, ct))
            {
                yield return row;
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> ApplyColumnarSegmentAsync(
        IAsyncEnumerable<RecordBatch> source,
        List<IDataTransformer> transformers,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in source)
        {
            var currentBatch = batch;

            foreach (var t in transformers)
            {
                var transCol = (IColumnarTransformer)t;
                var res = await transCol.TransformBatchAsync(currentBatch, ct);
                if (res != null)
                {
                    progress.ReportTransform(t.GetType().Name.Replace("DataTransformer", ""), res.Length);
                    currentBatch = res;
                }
            }
            yield return currentBatch;
        }

        // Process final flush from stateful transformers
        for (int i = 0; i < transformers.Count; i++)
        {
            var t = (IColumnarTransformer)transformers[i];
            await foreach (var flushedBatch in t.FlushBatchAsync(ct))
            {
                if (flushedBatch == null) continue;
                RecordBatch? current = flushedBatch;

                // Pass flushed batch through subsequent transformers
                for (int j = i + 1; j < transformers.Count; j++)
                {
                    if (current == null) break;
                    var nextT = (IColumnarTransformer)transformers[j];
                    current = await nextT.TransformBatchAsync(current, ct);
                    progress.ReportTransform(nextT.GetType().Name, current?.Length ?? 0);
                }

                if (current != null) yield return current;
            }
        }
    }

    private async IAsyncEnumerable<object?[]> ApplyRowSegmentAsync(IAsyncEnumerable<object?[]> source, List<IDataTransformer> transformers, IExportProgress progress, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var row in source)
        {
            var results = ProcessRowThroughTransformers(row, transformers, progress, ct);
            foreach (var r in results) yield return r;
        }

        // Process final flush from stateful transformers
        for (int i = 0; i < transformers.Count; i++)
        {
            var t = transformers[i];
            var flushedRows = t.Flush().ToList();
            if (flushedRows.Count > 0)
            {
                var remainingTransformers = transformers.Skip(i + 1).ToList();
                foreach (var fr in flushedRows)
                {
                    var results = ProcessRowThroughTransformers(fr, remainingTransformers, progress, ct);
                    foreach (var r in results) yield return r;
                }
            }
        }
    }

    private List<object?[]> ProcessRowThroughTransformers(object?[] row, List<IDataTransformer> p, IExportProgress progress, CancellationToken ct)
    {
        var currentRows = new List<object?[]> { row };
        foreach (var transformer in p)
        {
            var nextRows = new List<object?[]>();
            foreach (var r in currentRows)
            {
                if (transformer is IMultiRowTransformer multi)
                {
                    foreach (var res in multi.TransformMany(r))
                    {
                        if (res != null) { nextRows.Add(res); progress.ReportTransform(transformer.GetType().Name, 1); }
                    }
                }
                else
                {
                    var res = transformer.Transform(r);
                    if (res != null) { nextRows.Add(res); progress.ReportTransform(transformer.GetType().Name, 1); }
                }
            }
            currentRows = nextRows;
            if (currentRows.Count == 0) break;
        }
        return currentRows;
    }

    private async Task ConsumeColumnarStreamAsync(IAsyncEnumerable<RecordBatch> source, IColumnarDataWriter writer, IExportProgress progress, RetryPolicy retry, CancellationToken ct)
    {
        await foreach (var batch in source)
        {
            await retry.ExecuteValueAsync(() => writer.WriteRecordBatchAsync(batch, ct), ct);
            progress.ReportWrite(batch.Length);
        }
    }

    private async Task ConsumeRowStreamAsync(IAsyncEnumerable<object?[]> source, IDataWriter writer, int batchSize, IExportProgress progress, RetryPolicy retry, CancellationToken ct)
    {
        var buffer = new List<object?[]>(batchSize);
        await foreach (var row in source)
        {
            buffer.Add(row);
            if (buffer.Count >= batchSize)
            {
                var batch = buffer.ToArray();
                await retry.ExecuteValueAsync(() => writer.WriteBatchAsync(batch, ct), ct);
                progress.ReportWrite(batch.Length);
                buffer.Clear();
            }
        }
        if (buffer.Count > 0)
        {
            var batch = buffer.ToArray();
            await retry.ExecuteValueAsync(() => writer.WriteBatchAsync(batch, ct), ct);
            progress.ReportWrite(batch.Length);
        }
    }

    private async Task DirectRowTransferAsync(IStreamReader reader, IDataWriter writer, int batchSize, int limit, double samplingRate, int? samplingSeed, IExportProgress progress, RetryPolicy retry, CancellationToken ct)
    {
        long rowCount = 0;
        Random? sampler = samplingRate > 0 && samplingRate < 1.0 ? (samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared) : null;

        await foreach (var batch in reader.ReadBatchesAsync(batchSize, ct))
        {
            var rowsToWrite = new List<object?[]>();
            for (int i = 0; i < batch.Length; i++)
            {
                if (sampler != null && sampler.NextDouble() > samplingRate) continue;
                rowsToWrite.Add(batch.Span[i]);
                if (limit > 0 && ++rowCount >= limit) break;
            }

            if (rowsToWrite.Count > 0)
            {
                await retry.ExecuteValueAsync(() => writer.WriteBatchAsync(rowsToWrite.ToArray(), ct), ct);
                progress.ReportRead(rowsToWrite.Count);
                progress.ReportWrite(rowsToWrite.Count);
            }
            if (limit > 0 && rowCount >= limit) break;
        }
    }
}
