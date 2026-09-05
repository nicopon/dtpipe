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
using DtPipe.Configuration;
using Apache.Arrow.Types;
using DtPipe.Services;
using DtPipe.Core.Infrastructure.Retry;
using DtPipe.Services.Pipeline;

namespace DtPipe;

public class ExportService
{
	internal readonly IEnumerable<IStreamReaderFactory> _readerFactories;
	internal readonly IEnumerable<IDataWriterFactory> _writerFactories;
	internal readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
	internal readonly OptionsRegistry _optionsRegistry;
	internal readonly IExportObserver _observer;
	internal readonly IMemoryChannelRegistry? _channelRegistry;
	internal readonly ILogger<ExportService> _logger;
	internal readonly HookExecutor _hookExecutor;
	internal readonly MetricsService _metricsService;
	internal readonly SchemaValidationService _schemaValidator;
	internal readonly PipelineExecutor _pipelineExecutor;
	internal const int HookTimeoutSeconds = 30;

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
		bool showStatusMessages = false,
		bool quiet = false)
	{
		// Ensure the registry has the correct pipeline options for this run
		registry.Register(options);
		if (_logger.IsEnabled(LogLevel.Information))
			_logger.LogInformation("Starting export from {Provider} to {OutputPath}", providerName, ConnectionStringSanitizer.Sanitize(outputPath));

		// Silence internal DAG plumbing branches unless DEBUG=1 — capability check, not
		// an adapter-identity string comparison (F5). `quiet` folds in here: a run started for
		// a program (the MCP tools) suppresses the same chatter, and does so even under DEBUG,
		// because its console feeds a JSON reply or a screen the agent's toolkit owns.
		bool isInternalChannel = writerFactory is IInternalChannelCapable;
		bool silenceInternal = quiet || (isInternalChannel && Environment.GetEnvironmentVariable("DEBUG") != "1");
		bool outputIsStdio = string.Equals(outputPath, "-", StringComparison.Ordinal);

		if (showStatusMessages && !silenceInternal)
		{
			_observer.ShowIntro(providerName, outputPath);
			_observer.ShowConnectionStatus(false, null);
		}

		var retryPolicy = options.Retry
			? new DatabaseRetryPolicy(3, TimeSpan.FromSeconds(1))
			: (IRetryPolicy)NoRetryPolicy.Instance;
		await retryPolicy.ExecuteAsync(async retryCt =>
		{
			var state = new ExportRunState(this, options, providerName, outputPath, pipeline, readerFactory, writerFactory,
				registry, alias, resultsCollector, showStatusMessages, silenceInternal, outputIsStdio);
			await state.RunAsync(retryCt);
		}, ct);
	}

	/// <summary>
	/// Injects a compact Arrow schema JSON string into the reader's registered options
	/// via <c>Schema</c> (preferred) or falls back to <c>ColumnTypes</c> for CSV readers.
	/// </summary>
	internal static void InjectSchema(IStreamReaderFactory readerFactory, OptionsRegistry registry, string schemaJson)
	{
		var optType = readerFactory.GetSupportedOptionTypes().FirstOrDefault();
		if (optType == null) return;
		var opts = registry.Get(optType);

		if (opts is IHasSchemaOverride schemaOverride && string.IsNullOrEmpty(schemaOverride.Schema))
		{
			schemaOverride.Schema = schemaJson;
			registry.RegisterByType(optType, opts);
			return;
		}

		// Fallback for CSV (flat, row-based): extract scalar ColumnTypes from the schema.
		var columnTypesProp = optType.GetProperty("ColumnTypes");
		if (columnTypesProp != null && columnTypesProp.CanWrite
			&& string.IsNullOrEmpty(columnTypesProp.GetValue(opts) as string))
		{
			try
			{
				var schema = ArrowSchemaSerializer.Deserialize(schemaJson);
				var hints = schema.FieldsList
					.Select(f => (f.Name, Hint: ArrowTypeToColumnTypeHint(f)))
					.Where(x => !string.IsNullOrEmpty(x.Hint))
					.Select(x => $"{x.Name}:{x.Hint}");
				var columnTypes = string.Join(",", hints);
				if (!string.IsNullOrEmpty(columnTypes))
				{
					columnTypesProp.SetValue(opts, columnTypes);
					registry.RegisterByType(optType, opts);
				}
			}
			catch { /* best-effort */ }
		}
	}

	/// <summary>Extracts a --column-types hint string for a field (scalars only; null for complex types).</summary>
	private static string? ArrowTypeToColumnTypeHint(Field field)
	{
		// Check arrow.uuid metadata first
		if (field.Metadata?.TryGetValue("ARROW:extension:name", out var ext) == true && ext == "arrow.uuid")
			return "uuid";
		return field.DataType switch
		{
			StringType     => "string",
			Int32Type      => "int32",
			Int64Type      => "int64",
			FloatType      => "float32",
			DoubleType     => "float64",
			Decimal128Type => "decimal",
			BooleanType    => "bool",
			Date32Type     => "date32",
			Date64Type     => "datetime",
			TimestampType  => "datetimeoffset",
			_              => null
		};
	}

	internal static Schema EvolveSchema(Schema original, IReadOnlyList<PipeColumnInfo> transformed)
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

	internal static PipelineExecutionPlan BuildExecutionPlan(
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
