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
using DtPipe.Core.Pipelines;
using DtPipe.Services;

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
	private readonly HookExecutor _hookExecutor;
	private readonly MetricsService _metricsService;
	private readonly SchemaValidationService _schemaValidator;
	private readonly PipelineExecutor _pipelineExecutor;
	private const int HookTimeoutSeconds = 30;

	public ExportService(
		IEnumerable<IStreamReaderFactory> readerFactories,
		IEnumerable<IDataWriterFactory> writerFactories,
		IEnumerable<IDataTransformerFactory> transformerFactories,
		IEnumerable<IRowToColumnarBridgeFactory> bridgeFactories,
		IEnumerable<IColumnarToRowBridgeFactory> columnarToRowBridgeFactories,
		OptionsRegistry optionsRegistry,
		IExportObserver observer,
		ILogger<ExportService> logger,
		ILoggerFactory loggerFactory,
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
		_hookExecutor = new HookExecutor(observer, loggerFactory.CreateLogger<HookExecutor>());
		_metricsService = new MetricsService(observer, loggerFactory.CreateLogger<MetricsService>());
		_schemaValidator = new SchemaValidationService(observer, loggerFactory.CreateLogger<SchemaValidationService>());
		_pipelineExecutor = new PipelineExecutor(_bridgeFactories, _columnarToRowBridgeFactories, loggerFactory.CreateLogger<PipelineExecutor>());
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
		var segments = PipelineSegmenter.GetSegments(pipeline);
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
		if (!string.IsNullOrEmpty(alias) && _channelRegistry != null && _channelRegistry.ContainsChannel(alias))
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
					if (!string.IsNullOrEmpty(alias) && _channelRegistry != null && _channelRegistry.ContainsChannel(alias))
					{
						// Update registry with real schema and column info
						if (currentSchema != null)
						{
							_channelRegistry.UpdateChannelColumns(alias, currentSchema);
						}
						
						if (reader is IColumnarStreamReader cr && cr.Schema != null)
						{
							_channelRegistry.UpdateArrowChannelSchema(alias, cr.Schema);
						}
					}
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

		// Schema Validation
		await retryPolicy.ExecuteAsync(() => _schemaValidator.ValidateAndMigrateAsync(writer, exportableSchema, options, ct), ct);

		// Execute Pre-Hook
		await _hookExecutor.ExecuteAsync(writer, "Pre-Hook", options.PreExec, ct);

		await retryPolicy.ExecuteValueAsync(() => writer.InitializeAsync(exportableSchema, ct), ct);

		// Use Observer to create Progress
		var transformerNamesList = pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
		using var progress = _observer.CreateProgressReporter(!options.NoStats, transformerNamesList);

		using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var effectiveCt = linkedCts.Token;

		try
		{
			var startTime = DateTime.UtcNow;

			// Execute Unified Pipeline
			await _pipelineExecutor.ExecuteSegmentedPipelineAsync(reader, writer, segments, exportableSchema, options, progress, retryPolicy, linkedCts, effectiveCt);

			await writer.CompleteAsync(ct);
			progress.Complete();

			var elapsed = DateTime.UtcNow - startTime;
			var rowsPerSecond = elapsed.TotalSeconds > 0 ? progress.GetMetrics().ReadCount / elapsed.TotalSeconds : 0;
			if (_logger.IsEnabled(LogLevel.Information))
				_logger.LogInformation("Export completed in {Elapsed}. Written {Rows} rows ({Speed:F1} rows/s).", elapsed, progress.GetMetrics().WriteCount, rowsPerSecond);

			// --- POST-EXEC HOOK ---
			await _hookExecutor.ExecuteAsync(writer, "Post-Hook", options.PostExec, ct);

			_observer.LogMessage($"[green]✓ Export completed successfully.[/]");

			await _metricsService.SaveMetricsAsync(progress, options.MetricsPath, ct);
		}
		catch (OperationCanceledException)
		{
			// Graceful termination for orphaned producers or cancellation
			progress.Complete();
			_observer.LogMessage($"[grey]✓ Export stopped (no more consumers).[/]");
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Export failed");
			_observer.LogError(ex);

			try
			{
				await _hookExecutor.ExecuteAsync(writer, "On-Error Hook", options.OnErrorExec, CancellationToken.None, TimeSpan.FromSeconds(HookTimeoutSeconds));
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
			try
			{
				await _hookExecutor.ExecuteAsync(writer, "Finally Hook", options.FinallyExec, CancellationToken.None, TimeSpan.FromSeconds(HookTimeoutSeconds));
			}
			catch (Exception hookEx)
			{
				_logger.LogError(hookEx, "Finally Hook failed");
				_observer.LogError(hookEx);
			}
		}
	}
}
