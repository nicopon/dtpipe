using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
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
		OptionsRegistry optionsRegistry,
		IExportObserver observer,
		ILogger<ExportService> logger,
		HookExecutor hookExecutor,
		MetricsService metricsService,
		SchemaValidationService schemaValidator,
		PipelineExecutor pipelineExecutor,
		IMemoryChannelRegistry? channelRegistry = null)
	{
		_readerFactories = readerFactories;
		_writerFactories = writerFactories;
		_transformerFactories = transformerFactories;
		_optionsRegistry = optionsRegistry;
		_observer = observer;
		_logger = logger;
		_channelRegistry = channelRegistry;
		_hookExecutor = hookExecutor;
		_metricsService = metricsService;
		_schemaValidator = schemaValidator;
		_pipelineExecutor = pipelineExecutor;
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
		string? alias = null,
		System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector = null,
		bool showStatusMessages = false)
	{
		if (_logger.IsEnabled(LogLevel.Information))
			_logger.LogInformation("Starting export from {Provider} to {OutputPath}", providerName, ConnectionStringSanitizer.Sanitize(outputPath));

		// Silence internal DAG plumbing branches (arrow-memory / memory-channel) unless DEBUG=1
		bool isInternalChannel = writerFactory.ComponentName is "arrow-memory" or "memory-channel";
		bool silenceInternal = isInternalChannel && Environment.GetEnvironmentVariable("DEBUG") != "1";
		bool outputIsStdio = string.Equals(outputPath, "-", StringComparison.Ordinal);

		if (showStatusMessages && !silenceInternal)
		{
			_observer.ShowIntro(providerName, outputPath);
			_observer.ShowConnectionStatus(false, null);
		}

		await using var reader = readerFactory.Create(registry);
		await reader.OpenAsync(ct);

		// Show auto-applied types panel when --auto-column-types was set
		if (!silenceInternal && reader is IColumnTypeInferenceCapable autoCapable
		    && autoCapable.AutoAppliedTypes?.Count > 0)
			_observer.ShowColumnTypeInferenceSuggestion(autoCapable.AutoAppliedTypes, 100, applied: true);

		if (showStatusMessages && !silenceInternal)
			_observer.ShowConnectionStatus(true, reader.Columns?.Count);

		if (reader.Columns is null || reader.Columns.Count == 0)
		{
			if (!silenceInternal) _observer.LogWarning("No columns returned by query.");
			return;
		}

		// Initialize pipeline to define Target Schema
		var currentSchema = reader.Columns ?? System.Array.Empty<PipeColumnInfo>();
		var transformerSchemas = new Dictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)>();

		if (pipeline.Count > 0)
		{
			var transformerNames = pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
			if (showStatusMessages && !silenceInternal) _observer.ShowPipeline(transformerNames);

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
			// Register the Arrow channel once schema known
			if (!string.IsNullOrEmpty(alias))
			{
				Schema? sourceSchema = (reader as IStreamTransformer)?.Schema ?? (reader as IColumnarStreamReader)?.Schema;
				if (sourceSchema != null)
				{
					var evolvedSchema = EvolveSchema(sourceSchema, currentSchema);
					_channelRegistry.UpdateArrowChannelSchema(alias, evolvedSchema);
				}
			}

			// 2. Update row-based columns
			_channelRegistry.UpdateChannelColumns(alias, currentSchema ?? System.Array.Empty<PipeColumnInfo>());
		}

		// Column type inference advisory (dry-run only, for text sources like CSV)
		if (options.DryRunCount > 0 && reader is IColumnTypeInferenceCapable inferCapable)
		{
			try
			{
				var sampleCount = Math.Max(options.DryRunCount, 100);
				var suggested = await inferCapable.InferColumnTypesAsync(sampleCount, ct);
				if (suggested.Count > 0 && !silenceInternal)
					_observer.ShowColumnTypeInferenceSuggestion(suggested, sampleCount);
			}
			catch { /* inference is best-effort, never fail the dry-run */ }
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

			var executionPlan = BuildExecutionPlan(providerName, reader, writerFactory.ComponentName, writerForInspection, pipeline, segments);
		await _observer.RunDryRunAsync(reader, pipeline, options.DryRunCount, writerForInspection, transformerSchemas, executionPlan, ct);

			if (writerForInspection != null)
			{
				await writerForInspection.DisposeAsync();
			}
			return;
		}

		string writerName = writerFactory.ComponentName;
		if (showStatusMessages && !silenceInternal) _observer.ShowTarget(writerName, outputPath);

		var exportableSchema = currentSchema ?? throw new InvalidOperationException("Exportable schema is null.");
		await using var writer = writerFactory.Create(registry);

		// Schema Validation
		await _schemaValidator.ValidateAndMigrateAsync(writer, exportableSchema, options, ct);

		// Execute Pre-Hook
		await _hookExecutor.ExecuteAsync(writer, "Pre-Hook", options.PreExec, ct);

		await writer.InitializeAsync(exportableSchema, ct);

		// Use Observer to create Progress
		var transformerModes = segments
			.SelectMany(s => s.Transformers.Select(t => (
				Name: t.GetType().Name.Replace("DataTransformer", ""),
				IsColumnar: s.IsColumnar)))
			.ToList();
		using var progress = (silenceInternal && resultsCollector == null)
			? (IExportProgress)new DtPipe.Feedback.NullExportProgress()
			: _observer.CreateProgressReporter(
				!options.NoStats && !silenceInternal,
				transformerModes,
				suppressLiveTui: outputIsStdio || silenceInternal,
				branchName: alias,
				suppressCompletionOutput: resultsCollector != null);

		using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
		var effectiveCt = linkedCts.Token;

		// Propagate richSourceSchema for Arrow-capable readers (e.g. JsonL with StructType columns).
		// InputSchema and OutputSchema are already populated per-segment by ExportService above
		// (using the transformer input/output schema chain). Only set InputSchemaArrow here to pass
		// the native Arrow schema to the row→columnar bridge so it can preserve complex types.
		Schema? richSourceSchema = (reader as IStreamTransformer)?.Schema ?? (reader as IColumnarStreamReader)?.Schema;
		foreach (var segment in segments)
		{
			segment.InputSchemaArrow = richSourceSchema;
		}

		try
		{
			var startTime = DateTime.UtcNow;

			// Execute Unified Pipeline
			await _pipelineExecutor.ExecuteSegmentedPipelineAsync(reader, writer, segments, exportableSchema, options, progress, linkedCts, effectiveCt);

			await writer.CompleteAsync(ct);
			progress.Complete();

			resultsCollector?.Enqueue(new DtPipe.Feedback.BranchSummary(
				alias,
				progress.GetMetrics(),
				reader is DtPipe.Core.Abstractions.IColumnarStreamReader,
				transformerModes));

			var elapsed = DateTime.UtcNow - startTime;
			var rowsPerSecond = elapsed.TotalSeconds > 0 ? progress.GetMetrics().ReadCount / elapsed.TotalSeconds : 0;
			if (_logger.IsEnabled(LogLevel.Information))
				_logger.LogInformation("Export completed in {Elapsed}. Written {Rows} rows ({Speed:F1} rows/s).", elapsed, progress.GetMetrics().WriteCount, rowsPerSecond);

			// --- POST-EXEC HOOK ---
			await _hookExecutor.ExecuteAsync(writer, "Post-Hook", options.PostExec, ct);


			await _metricsService.SaveMetricsAsync(progress, options.MetricsPath, ct);
		}
		catch (OperationCanceledException)
		{
			// Graceful termination for orphaned producers or cancellation
			progress.Complete();
			if (!silenceInternal) _observer.LogMessage($"[grey]✓ Export stopped (no more consumers).[/]");
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

	private static Schema EvolveSchema(Schema original, IReadOnlyList<PipeColumnInfo> transformed)
	{
		var fields = new List<Field>(transformed.Count);
		foreach (var col in transformed)
		{
			// Try to find matching field in original schema to preserve nested types
			var originalField = original.FieldsList.FirstOrDefault(f => string.Equals(f.Name, col.Name, StringComparison.OrdinalIgnoreCase));
			if (originalField != null)
			{
				fields.Add(originalField);
			}
			else
			{
				// New field (e.g. from --fake), map from CLR type
				var arrowType = DtPipe.Core.Infrastructure.Arrow.ArrowTypeMapper.GetLogicalType(col.ClrType).ArrowType;
				fields.Add(new Field(col.Name, arrowType, col.IsNullable));
			}
		}
		return new Schema(fields, null);
	}

	private static PipelineExecutionPlan BuildExecutionPlan(
		string readerName,
		IStreamReader reader,
		string writerName,
		IDataWriter? writer,
		List<IDataTransformer> pipeline,
		List<PipelineSegment> segments)
	{
		bool readerIsColumnar = reader is IColumnarStreamReader;
		bool writerIsColumnar = writer is IColumnarDataWriter;
		bool rowModePreferred = !writerIsColumnar;

		var steps = new List<PipelineExecutionStep>(pipeline.Count);
		foreach (var segment in segments)
		{
			bool willRunColumnar = segment.IsColumnar && !rowModePreferred;
			foreach (var t in segment.Transformers)
			{
				steps.Add(new PipelineExecutionStep(
					t.GetType().Name.Replace("DataTransformer", ""),
					segment.IsColumnar,
					willRunColumnar));
			}
		}

		// Count mode-transition bridges
		int bridges = 0;
		bool current = readerIsColumnar && !rowModePreferred;
		foreach (var segment in segments)
		{
			bool useColumnar = segment.IsColumnar && !rowModePreferred;
			if (useColumnar != current) { bridges++; current = useColumnar; }
		}
		if (writerIsColumnar != current) bridges++;

		return new PipelineExecutionPlan(readerName, readerIsColumnar, writerName, writerIsColumnar, rowModePreferred, steps, bridges);
	}
}
