using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
using DtPipe.Core.Resilience;
using DtPipe.Core.Validation;
using Microsoft.Extensions.Logging;
using Spectre.Console;

namespace DtPipe;

public class ExportService
{
	private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
	private readonly IEnumerable<IDataWriterFactory> _writerFactories;
	private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
	private readonly OptionsRegistry _optionsRegistry;
	private readonly IExportObserver _observer;
	private readonly ILogger<ExportService> _logger;

	public ExportService(
		IEnumerable<IStreamReaderFactory> readerFactories,
		IEnumerable<IDataWriterFactory> writerFactories,
		IEnumerable<IDataTransformerFactory> transformerFactories,
		OptionsRegistry optionsRegistry,
		IExportObserver observer,
		ILogger<ExportService> logger)
	{
		_readerFactories = readerFactories;
		_writerFactories = writerFactories;
		_transformerFactories = transformerFactories;
		_optionsRegistry = optionsRegistry;
		_observer = observer;
		_logger = logger;
	}

	public async Task RunExportAsync(
		PipelineOptions options,
		string providerName,
		string outputPath,
		CancellationToken ct,
		List<IDataTransformer> pipeline,
		IStreamReaderFactory readerFactory,
		IDataWriterFactory writerFactory,
		OptionsRegistry registry)
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
		var currentSchema = reader.Columns;
		if (pipeline.Count > 0)
		{
			var transformerNames = pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
			_observer.ShowPipeline(transformerNames);

			foreach (var t in pipeline)
			{
				currentSchema = await t.InitializeAsync(currentSchema, ct);
			}
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
				catch { }
			}

			await _observer.RunDryRunAsync(reader, pipeline, options.DryRunCount, writerForInspection, ct);

			if (writerForInspection != null)
			{
				await writerForInspection.DisposeAsync();
			}
			return;
		}

		string writerName = writerFactory.ProviderName;
		_observer.ShowTarget(writerName, outputPath);

		var exportableSchema = currentSchema;
		await using var writer = writerFactory.Create(registry);

		// Execute Pre-Hook
		await ExecuteHookAsync(writer, "Pre-Hook", options.PreExec, ct);

		await retryPolicy.ExecuteValueAsync(() => writer.InitializeAsync(exportableSchema, ct), ct);

		// Schema Validation
		await retryPolicy.ExecuteAsync(() => ValidateAndMigrateSchemaAsync(writer, exportableSchema, options, ct), ct);

		// Bounded Channels
		// Reader to Transform remains row-level because transformers work row-by-row
		var readerToTransform = Channel.CreateBounded<object?[]>(new BoundedChannelOptions(1000)
		{
			SingleWriter = true,
			SingleReader = true,
			FullMode = BoundedChannelFullMode.Wait
		});

		// Transform to Writer becomes batch-level to reduce channel overhead
		// We reduce capacity because each item is now a batch of ~BatchSize rows
		var transformToWriter = Channel.CreateBounded<object?[][]>(new BoundedChannelOptions(100)
		{
			SingleWriter = true,
			SingleReader = true,
			FullMode = BoundedChannelFullMode.Wait
		});

		// Use Observer to create Progress
		var transformerNamesList = pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
		using var progress = _observer.CreateProgressReporter(!options.NoStats, transformerNamesList);

		long totalRows = 0;

		using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var effectiveCt = linkedCts.Token;

		try
		{
			// Run Concurrent Pipeline
			var startTime = DateTime.UtcNow;

			var producerTask = ProduceRowsAsync(reader, readerToTransform.Writer, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, linkedCts, effectiveCt, retryPolicy, _logger);
			var transformTask = TransformRowsAsync(readerToTransform.Reader, transformToWriter.Writer, pipeline, options.BatchSize, progress, effectiveCt);
			var consumerTask = ConsumeRowsAsync(transformToWriter.Reader, writer, progress, r => Interlocked.Add(ref totalRows, r), effectiveCt, retryPolicy, _logger);

			var tasks = new List<Task> { producerTask, transformTask, consumerTask };

			while (tasks.Count > 0)
			{
				var finishedTask = await Task.WhenAny(tasks);
				if (finishedTask.IsFaulted)
				{
					await linkedCts.CancelAsync();
					await finishedTask;
				}
				else if (finishedTask.IsCanceled)
				{
					await linkedCts.CancelAsync();
				}

				tasks.Remove(finishedTask);
			}

			await writer.CompleteAsync(ct);
			progress.Complete();

			var elapsed = DateTime.UtcNow - startTime;
			var rowsPerSecond = elapsed.TotalSeconds > 0 ? totalRows / elapsed.TotalSeconds : 0;
			if (_logger.IsEnabled(LogLevel.Information))
				_logger.LogInformation("Export completed in {Elapsed}. Written {Rows} rows ({Speed:F1} rows/s).", elapsed, totalRows, rowsPerSecond);

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

    /// <summary>
    /// Producer: Reads batches from database, unbatches them, and sends single rows to channel.
    /// </summary>
    private static async Task ProduceRowsAsync(
        IStreamReader reader,
        ChannelWriter<object?[]> output,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        CancellationTokenSource linkedCts,
        CancellationToken ct,
        RetryPolicy retryPolicy,
        ILogger logger)
    {
        logger.LogDebug("Producer/Reader started");

        // Sampling initialization
        Random? sampler = null;
        if (samplingRate > 0 && samplingRate < 1.0)
        {
            sampler = samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared;
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Data sampling enabled: {Rate:P0} (Seed: {Seed})", samplingRate, samplingSeed.HasValue ? samplingSeed.Value.ToString() : "Auto");
        }

        long rowCount = 0;
        try
        {
            // Note: We don't wrap the entire foreach because ReadBatchesAsync is an IAsyncEnumerable.
            // If we wanted to retry the whole stream, we'd need to reopen the reader.
            // However, individual batch reads could be retried IF the reader supported it inside,
            // but here we just wrap the loop if possible, or better, we wrap the WriteAsync if needed.
            // Actually, for readers, transient errors usually happen during the Fetch.

            await foreach (var batchChunk in reader.ReadBatchesAsync(batchSize, ct))
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("Read batch of {Count} rows", batchChunk.Length);
                for (var i = 0; i < batchChunk.Length; i++)
                {
                    if (sampler != null && sampler.NextDouble() > samplingRate)
                    {
                        continue;
                    }

                    await output.WriteAsync(batchChunk.Span[i], ct);
                    progress.ReportRead(1);
                    rowCount++;

                    if (limit > 0 && rowCount >= limit)
                    {
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("Limit of {Limit} rows reached. Stopping producer.", limit);
                        return;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (limit > 0 && rowCount >= limit)
        {
            // Expected
        }
        finally
        {
            output.Complete();
        }
    }

    private static async Task TransformRowsAsync(
        ChannelReader<object?[]> input,
        ChannelWriter<object?[][]> output,
        IReadOnlyList<IDataTransformer> pipeline,
        int batchSize,
        IExportProgress progress,
        CancellationToken ct)
    {
        try
        {
            await using (var batchWriter = new BatchChannelWriter(output, batchSize, ct))
            {
                await foreach (var row in input.ReadAllAsync(ct))
                {
                    await ProcessPipelineAsync(row, 0, pipeline, batchWriter, progress, ct);
                }

                // Flush pipeline
                for (int i = 0; i < pipeline.Count; i++)
                {
                    var transformer = pipeline[i];
                    var flushedRows = transformer.Flush();
                    foreach (var row in flushedRows)
                    {
                        if (row != null)
                        {
                            await ProcessPipelineAsync(row, i + 1, pipeline, batchWriter, progress, ct);
                        }
                    }
                }
            } // batchWriter disposes and flushes here
        }
        finally
        {
            output.Complete();
        }
    }

    private static async ValueTask ProcessPipelineAsync(
        object?[] currentRow,
        int stepIndex,
        IReadOnlyList<IDataTransformer> pipeline,
        BatchChannelWriter finalOutput,
        IExportProgress progress,
        CancellationToken ct)
    {
        // Base case: writing to the batching buffer
        if (stepIndex >= pipeline.Count)
        {
            await finalOutput.WriteAsync(currentRow);
            return;
        }

        var transformer = pipeline[stepIndex];
        var transformerName = transformer.GetType().Name;

        if (transformer is IMultiRowTransformer multiTransformer)
        {
            var results = multiTransformer.TransformMany(currentRow);

            foreach (var resultRow in results)
            {
                if (resultRow != null)
                {
                    progress.ReportTransform(transformerName, 1);
                    await ProcessPipelineAsync(resultRow, stepIndex + 1, pipeline, finalOutput, progress, ct);
                }
            }
        }
        else
        {
            var resultRow = transformer.Transform(currentRow);
            if (resultRow != null)
            {
                progress.ReportTransform(transformerName, 1);
                await ProcessPipelineAsync(resultRow, stepIndex + 1, pipeline, finalOutput, progress, ct);
            }
        }
    }

    /// <summary>
    /// Consumer: Reads batches and writes them directly to destination.
    /// </summary>
    private static async Task ConsumeRowsAsync(
        ChannelReader<object?[][]> input,
        IDataWriter writer,
        IExportProgress progress,
        Action<int> updateRowCount,
        CancellationToken ct,
        RetryPolicy retryPolicy,
        ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Consumer/Writer started");

        await foreach (var batch in input.ReadAllAsync(ct))
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Writing batch of {Count} rows", batch.Length);
            }

            await retryPolicy.ExecuteValueAsync(() => writer.WriteBatchAsync(batch, ct), ct);

            updateRowCount(batch.Length);
            progress.ReportWrite(batch.Length);

            LogMemoryUsage(logger);
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
}
