using System.Reflection;
using System.Runtime.CompilerServices;
using DtPipe.Cli;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Transformers.Services;
using DtPipe.Observers;
using DtPipe.Transformers.Arrow.Fake;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Arrow.Format;
using DtPipe.Transformers.Arrow.Mask;
using DtPipe.Transformers.Arrow.Null;
using DtPipe.Transformers.Arrow.Overwrite;
using DtPipe.Transformers.Arrow.Project;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;

[assembly: InternalsVisibleTo("DtPipe.Tests")]
namespace DtPipe;

class Program
{
	internal static async Task<int> Main(string[] args)
	{
		System.Globalization.CultureInfo.DefaultThreadCurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
		System.Globalization.CultureInfo.DefaultThreadCurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;
		Console.OutputEncoding = System.Text.Encoding.UTF8;

		Log.Logger = new LoggerConfiguration()
				.MinimumLevel.Debug()
				.WriteTo.Console(
				outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
				standardErrorFromLevel: Serilog.Events.LogEventLevel.Verbose)
				.CreateLogger();

		var services = new ServiceCollection();
		ConfigureServices(services);
		var serviceProvider = services.BuildServiceProvider();

		var jobService = serviceProvider.GetRequiredService<JobService>();

		// Ensure cursor is restored on Ctrl+C
		Console.CancelKeyPress += (_, _) => Console.CursorVisible = true;

		try
		{
			// 0. Version check
			if (args.Length > 0 && args[0] == "--version")
			{
				var assembly = Assembly.GetExecutingAssembly();
				var version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
					?? assembly.GetName().Version?.ToString()
					?? "unknown";
				Console.WriteLine($"dtpipe {version}");
				return 0;
			}

			// 1. Shell completion — intercept [suggest] before any other dispatch
			if (args.Length > 0 && args[0] == "[suggest]")
				return HandleSuggest(args, serviceProvider);

			// 2. Detect Subcommand vs Pipeline
			if (args.Length > 0 && IsSubcommand(args[0]))
			{
				var rootCommand = jobService.BuildSubcommands();
				return await rootCommand.Parse(args).InvokeAsync();
			}

			// 3. Help Interception
			if (args.Length == 0 || args.Any(a => a == "--help" || a == "-h" || a == "-?"))
			{
				HelpRenderer.Print(serviceProvider);
				return 0;
			}

			// 4. Pipeline Execution (New Parser)
			var registry = FlagRegistryFactory.Build(serviceProvider);
			var streamTransformerFactories = serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();

			var lexer = new PipelineLexer(registry);
			var parsedPipeline = lexer.Parse(args);
			var secretsManager = serviceProvider.GetRequiredService<DtPipe.Cli.Security.ISecretsManager>();
			var (jobs, dag, contexts) = PipelineToJobConverter.Convert(parsedPipeline, streamTransformerFactories, secretsManager);

			if (!string.IsNullOrEmpty(parsedPipeline.Globals.ExportJobFile))
			{
				DtPipe.Configuration.JobFileWriter.Write(parsedPipeline.Globals.ExportJobFile, jobs);
				return 0;
			}

			return await jobService.ExecutePipelineAsync(jobs, dag, contexts, parsedPipeline.Globals, CancellationToken.None);
		}
		catch (Exception ex)
		{
			Console.Error.WriteLine($"[ERROR] {ex.Message}");
			if (Environment.GetEnvironmentVariable("DEBUG") == "1") Console.Error.WriteLine(ex.StackTrace);
			return 1;
		}
		finally
		{
			try { Console.CursorVisible = true; } catch { }
			await Log.CloseAndFlushAsync();
		}
	}

	private static bool IsSubcommand(string arg)
	{
		var subs = new[] { "inspect", "providers", "completion", "secret", "mcp" };
		return subs.Contains(arg.ToLowerInvariant());
	}

	/// <summary>
	/// Stage-aware shell completion handler.
	/// Protocol: dtpipe [suggest] &lt;cursor_word_index&gt; &lt;word1&gt; &lt;word2&gt; ...
	/// Emits one completion candidate per line (with [NOSUSP] suffix to suppress space for prefix values).
	/// </summary>
	private static int HandleSuggest(string[] args, IServiceProvider sp)
	{
		// args = ["[suggest]", "<cursor_pos>", "<word1>", ...]
		if (args.Length < 2 || !int.TryParse(args[1], out var cursorPos)) return 0;
		var words = args.Skip(2).ToArray();          // words typed so far (excluding dtpipe itself)
		var partial = cursorPos <= words.Length       // word currently being typed (may be incomplete)
			 ? words.ElementAtOrDefault(cursorPos - 1) ?? ""
			 : "";
		var precedingWords = words.Take(Math.Max(0, cursorPos - 1)).ToArray();

		// Build the unified FlagRegistry (shared with Main pipeline execution)
		var registry = FlagRegistryFactory.Build(sp);
		var readerFactories    = sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();
		var writerFactories    = sp.GetRequiredService<IEnumerable<IDataWriterFactory>>().ToList();
		var transformerFactories = sp.GetRequiredService<IEnumerable<IDataTransformerFactory>>().ToList();

		// Determine current stage by scanning preceding words.
		// Stage transitions: Reader (default) → Pipeline (on first transformer trigger) → Writer (on -o).
		var currentStage = FlagStage.Reader;
		string? activeReaderProvider = null;
		string? activeWriterProvider = null;
		string? lastPipelineTrigger  = null;
		bool valuePending = false; // true when the next word is a flag value (not a flag)

		for (int i = 0; i < precedingWords.Length; i++)
		{
			var w = precedingWords[i];
			if (valuePending) { valuePending = false; continue; } // skip flag values

			if (w == "-i" || w == "--input")
			{
				currentStage         = FlagStage.Reader;
				activeReaderProvider = precedingWords.ElementAtOrDefault(i + 1);
				valuePending      = true;
			}
			else if (w == "-o" || w == "--output")
			{
				currentStage         = FlagStage.Writer;
				activeWriterProvider = precedingWords.ElementAtOrDefault(i + 1);
				valuePending      = true;
			}
			else
			{
				var def = registry.Lookup(w);
				if (def?.Stage == FlagStage.Pipeline && currentStage == FlagStage.Reader)
				{
					currentStage        = FlagStage.Pipeline;
					lastPipelineTrigger = w;
				}
				if (def != null && def.Arity != FlagArity.Boolean) valuePending = true;
			}
		}

		// Resolve the active provider prefix (e.g. "pg" from "pg:host=...")
		static string? ExtractProviderPrefix(string? connStr) =>
			connStr != null && connStr.Contains(':') ? connStr.Split(':')[0].ToLowerInvariant() : null;

		var readerPrefix = ExtractProviderPrefix(activeReaderProvider);
		var writerPrefix = ExtractProviderPrefix(activeWriterProvider);

		// Collect candidates for the current stage
		var candidates = new List<string>();

		if (currentStage == FlagStage.Reader)
		{
			// Global/structural flags always available
			foreach (var def in registry.GetAll().Where(d => d.Stage == FlagStage.All && d.Name.StartsWith("--")))
				candidates.Add(def.Name);

			// Reader flags for the active provider (or all readers if no provider yet)
			IEnumerable<ICliContributor> activeReaders = readerPrefix != null
				 ? readerFactories.Where(f => f.ComponentName == readerPrefix).OfType<ICliContributor>()
				 : readerFactories.OfType<ICliContributor>();
			foreach (var c in activeReaders)
				foreach (var def in c.GetFlagDefs()) candidates.Add(def.Name);

			// Transformer triggers (to transition to Pipeline stage)
			foreach (var def in registry.GetAll().Where(d => d.Stage == FlagStage.Pipeline))
				candidates.Add(def.Name);

			// -o to transition to Writer stage
			candidates.Add("-o");

			// Provider prefixes when partial doesn't look like a flag
			if (partial == "" || !partial.StartsWith("-"))
				foreach (var f in readerFactories) candidates.Add(f.ComponentName + ":[NOSUSP]");
		}
		else if (currentStage == FlagStage.Pipeline)
		{
			// Flags for the active transformer
			var triggerDef = lastPipelineTrigger != null ? registry.Lookup(lastPipelineTrigger) : null;
			var activeTFactory = transformerFactories
				 .FirstOrDefault(c => c.ComponentName == triggerDef?.Description)
				as ICliContributor;
			if (activeTFactory != null)
				foreach (var def in activeTFactory.GetFlagDefs()) candidates.Add(def.Name);

			// Additional pipeline triggers
			foreach (var def in registry.GetAll().Where(d => d.Stage == FlagStage.Pipeline))
				candidates.Add(def.Name);

			// -o to end pipeline
			candidates.Add("-o");
		}
		else // Writer
		{
			// Writer flags for the active provider (or all writers if no provider yet)
			IEnumerable<ICliContributor> activeWriters = writerPrefix != null
				 ? writerFactories.Where(f => f.ComponentName == writerPrefix).OfType<ICliContributor>()
				 : writerFactories.OfType<ICliContributor>();
			foreach (var c in activeWriters)
				foreach (var def in c.GetFlagDefs()) candidates.Add(def.Name);

			// Global flags
			foreach (var def in registry.GetAll().Where(d => d.Stage == FlagStage.All && d.Name.StartsWith("--")))
				candidates.Add(def.Name);

			// Provider prefixes after -o
			if (partial == "" || !partial.StartsWith("-"))
				foreach (var f in writerFactories) candidates.Add(f.ComponentName + ":[NOSUSP]");
		}

		// Filter to prefix match and deduplicate, then emit
		var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		foreach (var c in candidates)
		{
			var bare = c.Replace("[NOSUSP]", "");
			if (!seen.Add(bare)) continue;
			if (!string.IsNullOrEmpty(partial) && !bare.StartsWith(partial, StringComparison.OrdinalIgnoreCase)) continue;
			Console.WriteLine(c);
		}
		return 0;
	}

	private static void ConfigureServices(IServiceCollection services)
	{
		services.AddLogging(logging => {
			logging.ClearProviders();
			logging.AddSerilog(Log.Logger);
			logging.SetMinimumLevel(LogLevel.Debug);
		});

		services.AddSingleton<OptionsRegistry>();
		services.AddSingleton<Spectre.Console.IAnsiConsole>(sp => Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings { Out = new Spectre.Console.AnsiConsoleOutput(Console.Error) }));
		services.AddSingleton<JobService>();

		var useFakeKeyring = Environment.GetEnvironmentVariable("DTPIPE_UNSAFE_INSECURE_FAKE_KEYRING");
		if (useFakeKeyring == "1" || useFakeKeyring == "true")
		{
			var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
			var tempPath = Path.Combine(appData, "dtpipe", "fake_keyring.json");
			services.AddSingleton<DtPipe.Cli.Security.ISecretsManager>(sp =>
				new DtPipe.Cli.Security.FileSecretsManager(tempPath, sp.GetRequiredService<ILogger<DtPipe.Cli.Security.FileSecretsManager>>()));
		}
		else
		{
			services.AddSingleton<DtPipe.Cli.Security.ISecretsManager, DtPipe.Cli.Security.KeyringSecretsManager>();
		}

		services.AddSingleton<DtPipe.Core.Security.IStringContentResolver, DtPipe.Cli.Security.CliStringContentResolver>();

		RegisterReader<DtPipe.Adapters.Arrow.ArrowReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Csv.CsvReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.DuckDB.DuckDbReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Generate.GenerateReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.JsonL.JsonLReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.MemoryChannel.ArrowMemoryChannelReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.MemoryChannel.MemoryChannelReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Oracle.OracleReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Parquet.ParquetReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.PostgreSQL.PostgreSqlReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Sqlite.SqliteReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.SqlServer.SqlServerReaderDescriptor>(services);
		RegisterReader<DtPipe.Adapters.Xml.XmlReaderDescriptor>(services);

		RegisterWriter<DtPipe.Adapters.Arrow.ArrowWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Checksum.ChecksumWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Csv.CsvWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.DuckDB.DuckDbWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.JsonL.JsonLWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.MemoryChannel.ArrowMemoryChannelWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.MemoryChannel.MemoryChannelWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Null.NullDataWriterFactory>(services);
		RegisterWriter<DtPipe.Adapters.Oracle.OracleWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Parquet.ParquetWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.PostgreSQL.PostgreSqlWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.Sqlite.SqliteWriterDescriptor>(services);
		RegisterWriter<DtPipe.Adapters.SqlServer.SqlServerWriterDescriptor>(services);

		RegisterStreamTransformer<DtPipe.Processors.Sql.CompositeSqlTransformerFactory>(services);
		RegisterStreamTransformer<DtPipe.Processors.Merge.MergeTransformerFactory>(services);

		RegisterTransformer<NullDataTransformerFactory>(services);
		RegisterTransformer<OverwriteDataTransformerFactory>(services);
		RegisterTransformer<FakeDataTransformerFactory>(services);
		RegisterTransformer<FormatDataTransformerFactory>(services);
		RegisterTransformer<MaskDataTransformerFactory>(services);
		RegisterTransformer<ComputeDataTransformerFactory>(services);
		RegisterTransformer<FilterDataTransformerFactory>(services);
		RegisterTransformer<ExpandDataTransformerFactory>(services);
		RegisterTransformer<ProjectDataTransformerFactory>(services);
		RegisterTransformer<WindowDataTransformerFactory>(services);

		services.AddSingleton<IJsEngineProvider, JsEngineProvider>();
		services.AddSingleton<IExportObserver, SpectreConsoleObserver>();
		services.AddSingleton<DtPipe.Services.HookExecutor>();
		services.AddSingleton<DtPipe.Services.MetricsService>();
		services.AddSingleton<DtPipe.Services.SchemaValidationService>();
		services.AddSingleton<DtPipe.Services.PipelineExecutor>();
		services.AddSingleton<ExportService>();
		services.AddSingleton<IMemoryChannelRegistry, MemoryChannelRegistry>();
		services.AddTransient<IDagOrchestrator, DagOrchestrator>();
		services.AddSingleton<IRowToColumnarBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory>();
		services.AddSingleton<IColumnarToRowBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory>();

		// MCP Server integration
		services.AddSingleton<DtPipe.Cli.Mcp.DtPipeMcpTools>();
		services.AddMcpServer()
		        .WithStdioServerTransport()
		        .WithTools<DtPipe.Cli.Mcp.DtPipeMcpTools>();
	}

	private static void RegisterStreamTransformer<TFac>(IServiceCollection services) where TFac : class, IStreamTransformerFactory => services.AddSingleton<IStreamTransformerFactory>(sp => ActivatorUtilities.CreateInstance<TFac>(sp));
	private static void RegisterWriter<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IDataWriter>, new()
	{
		var descriptor = new TDesc();
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(descriptor, sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<ICliContributor>(sp => (ICliContributor)sp.GetServices<IDataWriterFactory>().First(f => f.ComponentName == descriptor.ComponentName));
	}
	private static void RegisterReader<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IStreamReader>, new()
	{
		var descriptor = new TDesc();
		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(descriptor, sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<ICliContributor>(sp => (ICliContributor)sp.GetServices<IStreamReaderFactory>().First(f => f.ComponentName == descriptor.ComponentName));
	}
	private static void RegisterTransformer<TFac>(IServiceCollection services) where TFac : class, IDataTransformerFactory 
	{
		services.AddSingleton<IDataTransformerFactory>(sp => new CliDataTransformerFactory(ActivatorUtilities.CreateInstance<TFac>(sp)));
	}
}
