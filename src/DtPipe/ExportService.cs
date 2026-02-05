using System.Threading.Channels;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Feedback;
using Spectre.Console;
using Microsoft.Extensions.Logging;

namespace DtPipe;

public class ExportService
{
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;
    private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
    private readonly OptionsRegistry _optionsRegistry;
    private readonly IAnsiConsole _console;
    private readonly ILogger<ExportService> _logger;

    public ExportService(
        IEnumerable<IStreamReaderFactory> readerFactories, 
        IEnumerable<IDataWriterFactory> writerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        OptionsRegistry optionsRegistry,
        IAnsiConsole console,
        ILogger<ExportService> logger)
    {
        _readerFactories = readerFactories;
        _writerFactories = writerFactories;
        _transformerFactories = transformerFactories;
        _optionsRegistry = optionsRegistry;
        _console = console;
        _logger = logger;
    }

    public async Task RunExportAsync(
        DtPipe.Configuration.DumpOptions options, 
        CancellationToken ct, 
        List<IDataTransformer> pipeline,
        IStreamReaderFactory readerFactory,
        IDataWriterFactory writerFactory)
    {
        _logger.LogInformation("Starting export from {Provider} to {OutputPath}", options.Provider, options.OutputPath);

        // Display Source Info
        var table = new Table();
        table.Border(TableBorder.None);
        table.AddColumn(new TableColumn("[grey]Source[/]").RightAligned());
        table.AddColumn(new TableColumn($"[blue]{options.Provider}[/]"));
        _console.Write(table);

        _console.MarkupLine($"   [grey]Connecting...[/]");
        
        await using var reader = readerFactory.Create(options);
        
        await reader.OpenAsync(ct);
        
        _console.MarkupLine($"   [grey]Connected. Schema: [green]{reader.Columns?.Count ?? 0}[/] columns.[/]");        
        
        if (reader.Columns is null || reader.Columns.Count == 0)
        {
            _console.MarkupLine("[red]No columns returned by query.[/]");
            return;
        }

        // Initialize pipeline to define Target Schema
        var currentSchema = reader.Columns;
        if (pipeline.Count > 0)
        {
             _console.WriteLine();
             _console.Write(new Rule("[yellow]Pipeline[/]").LeftJustified());
             var grid = new Grid();
             grid.AddColumn();
             foreach(var t in pipeline)
             {
                 var name = t.GetType().Name.Replace("DataTransformer", "");
                 grid.AddRow($"[yellow]↓[/] [cyan]{name}[/]");
             }
             _console.Write(grid);
             
             foreach (var t in pipeline)
             {
                 currentSchema = await t.InitializeAsync(currentSchema, ct);
             }
        }

        // Dry-run mode: Delegate to DryRunService
        if (options.DryRunCount > 0)
        {
            // Try to create writer for schema inspection (if output is specified)
            IDataWriter? writerForInspection = null;
            if (!string.IsNullOrEmpty(options.OutputPath))
            {
               // We reuse the resolved writerFactory for inspection
               try 
               {
                   writerForInspection = writerFactory.Create(options);
               }
               catch 
               {
                   // Ignore if we can't create it for inspection (e.g. if it requires valid connection string and we have dry run logic)
               }
            }

            var dryRunController = new Cli.DryRun.DryRunCliController(_console);
            await dryRunController.RunAsync(reader, pipeline, options.DryRunCount, writerForInspection, ct);
            
            // Dispose writer if created
            if (writerForInspection != null)
            {
                await writerForInspection.DisposeAsync();
            }
            return;
        }

        string writerName = writerFactory.ProviderName;
        // Display Target Info
        var targetTable = new Table();
        targetTable.Border(TableBorder.None);
        targetTable.AddColumn(new TableColumn("[grey]Target[/]").RightAligned());
        targetTable.AddColumn(new TableColumn($"[blue]{writerName}[/]"));
        
        // Show output path if it's likely a file or simple string
        if (!string.IsNullOrEmpty(options.OutputPath)) 
        {
             targetTable.AddColumn(new TableColumn($"([grey]{options.OutputPath}[/])"));
        }
        
        _console.Write(targetTable);

        // --- PRE-EXEC HOOK ---
        if (!string.IsNullOrWhiteSpace(options.PreExec))
        {
            _console.MarkupLine($"   [yellow]Executing Pre-Hook...[/]");
            try 
            {
                // Ensure writer is created if not already (it is created below, we need strict ordering)
                // We create writer here, executed hook, then Initialize
            } 
            catch {} 
        }

        var exportableSchema = currentSchema;
        await using var writer = writerFactory.Create(options);

        // Execute Pre-Hook (Before Initialize, as it might create objects)
         if (!string.IsNullOrWhiteSpace(options.PreExec))
        {
            _console.MarkupLine($"   [yellow]Executing Pre-Hook: {Markup.Escape(options.PreExec)}[/]");
            await writer.ExecuteCommandAsync(options.PreExec, ct);
        }

        await writer.InitializeAsync(exportableSchema, ct);

        // Bounded Channels for backpressure
        var readerToTransform = Channel.CreateBounded<object?[]>(new BoundedChannelOptions(1000)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        var transformToWriter = Channel.CreateBounded<object?[]>(new BoundedChannelOptions(1000)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        using var progress = new ProgressReporter(_console, true, pipeline);
        long totalRows = 0;

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var effectiveCt = linkedCts.Token;

        try
        {
            // Run Concurrent Pipeline
            var startTime = DateTime.UtcNow;
            var producerTask = ProduceRowsAsync(reader, readerToTransform.Writer, options.BatchSize, options.Limit, options.SampleRate, options.SampleSeed, progress, linkedCts, effectiveCt, _logger);
            var transformTask = TransformRowsAsync(readerToTransform.Reader, transformToWriter.Writer, pipeline, progress, effectiveCt);
            var consumerTask = ConsumeRowsAsync(transformToWriter.Reader, writer, options.BatchSize, progress, r => Interlocked.Add(ref totalRows, r), effectiveCt, _logger);

            var tasks = new List<Task> { producerTask, transformTask, consumerTask };
            
            while (tasks.Count > 0)
            {
                 var finishedTask = await Task.WhenAny(tasks);
                 if (finishedTask.IsFaulted)
                 {
                     // If any task fails, cancel the others to prevent deadlock/hanging
                     await linkedCts.CancelAsync();
                     // Re-await the failed task to propagate exception
                     await finishedTask; 
                 }
                 else if (finishedTask.IsCanceled)
                 {
                     // If one task is cancelled (e.g. limit reached), ensure others stop
                     await linkedCts.CancelAsync();
                 }
                 
                 tasks.Remove(finishedTask);
            }

            await writer.CompleteAsync(ct);
            progress.Complete();

            var elapsed = DateTime.UtcNow - startTime;
            var rowsPerSecond = elapsed.TotalSeconds > 0 ? totalRows / elapsed.TotalSeconds : 0;
            _logger.LogInformation("Export completed in {Elapsed}. Written {Rows} rows ({Speed:F1} rows/s).", 
                elapsed, totalRows, rowsPerSecond);

            // --- POST-EXEC HOOK ---
            if (!string.IsNullOrWhiteSpace(options.PostExec))
            {
                _console.MarkupLine($"   [yellow]Executing Post-Hook: {Markup.Escape(options.PostExec)}[/]");
                await writer.ExecuteCommandAsync(options.PostExec, ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Export failed");
            Console.Error.WriteLine($"Export failed: {ex.Message}");
            
            // --- ON-ERROR HOOK ---
            if (!string.IsNullOrWhiteSpace(options.OnErrorExec))
            {
                try
                {
                    _console.MarkupLine($"   [red]Executing On-Error Hook: {Markup.Escape(options.OnErrorExec)}[/]");
                    // Use a new token (short timeout) to ensure it runs even if original CT is processing cancellation
                    using var errorCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    await writer.ExecuteCommandAsync(options.OnErrorExec, errorCts.Token);
                }
                catch (Exception hookEx)
                {
                    _logger.LogError(hookEx, "On-Error Hook failed");
                    Console.Error.WriteLine($"On-Error Hook failed: {hookEx.Message}");
                }
            }

            throw;
        }
        finally
        {
            // --- FINALLY HOOK ---
            if (!string.IsNullOrWhiteSpace(options.FinallyExec))
            {
                try
                {
                    _console.MarkupLine($"   [yellow]Executing Finally Hook: {Markup.Escape(options.FinallyExec)}[/]");
                    // Similar to On-Error, ensure it runs
                    using var finallyCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    await writer.ExecuteCommandAsync(options.FinallyExec, finallyCts.Token);
                }
                catch (Exception hookEx)
                {
                     _logger.LogError(hookEx, "Finally Hook failed");
                     Console.Error.WriteLine($"Finally Hook failed: {hookEx.Message}");
                }
            }
        }
    }

    /// <summary>
    /// Producer: Reads batches from database, unbatches them, and sends single rows to channel.
    /// This keeps DB I/O efficient (batched) while allowing downstream streaming.
    /// </summary>
    private static async Task ProduceRowsAsync(
        IStreamReader reader, 
        ChannelWriter<object?[]> output, 
        int batchSize,
        int limit,
        double sampleRate,
        int? sampleSeed,
        ProgressReporter progress,
        CancellationTokenSource linkedCts,
        CancellationToken ct,
        ILogger logger)
    {
        logger.LogDebug("Producer/Reader started");
        
        // Sampling initialization
        Random? sampler = null;
        if (sampleRate > 0 && sampleRate < 1.0)
        {
            sampler = sampleSeed.HasValue ? new Random(sampleSeed.Value) : Random.Shared;
            logger.LogInformation("Data sampling enabled: {Rate:P0} (Seed: {Seed})", sampleRate, sampleSeed.HasValue ? sampleSeed.Value.ToString() : "Auto");
        }

        long rowCount = 0;
        try
        {
            await foreach (var batchChunk in reader.ReadBatchesAsync(batchSize, ct))
            {
                logger.LogDebug("Read batch of {Count} rows", batchChunk.Length);
                for (var i = 0; i < batchChunk.Length; i++)
                {
                    // Apply Sampling High-Performance Filter
                    if (sampler != null && sampler.NextDouble() > sampleRate)
                    {
                        continue;
                    }

                    await output.WriteAsync(batchChunk.Span[i], ct);
                    progress.ReportRead(1);
                    rowCount++;

                    // Check limit and cancel if reached
                    if (limit > 0 && rowCount >= limit)
                    {
                        logger.LogInformation("Limit of {Limit} rows reached. Stopping producer.", limit);
                        return;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (limit > 0 && rowCount >= limit)
        {
            // Expected cancellation due to limit reached
        }
        finally
        {
            output.Complete();
        }
    }

    /// <summary>
    /// Transform stage: Applies all transformers in sequence to each row individually.
    /// </summary>
    private static async Task TransformRowsAsync(
        ChannelReader<object?[]> input,
        ChannelWriter<object?[]> output,
        IReadOnlyList<IDataTransformer> pipeline,
        ProgressReporter progress,
        CancellationToken ct)
    {
        try
        {
            await foreach (var row in input.ReadAllAsync(ct))
            {
                var workingRow = row;
                
                // Pure streaming transformation
                foreach (var transformer in pipeline)
                {
                    workingRow = transformer.Transform(workingRow);
                    progress.ReportTransform(transformer.GetType().Name, 1);
                }
                
                await output.WriteAsync(workingRow, ct);
            }
        }
        finally
        {
            output.Complete();
        }
    }

    /// <summary>
    /// Consumer: Accumulates rows into batches and writes them to output file.
    /// efficiently handling file I/O.
    /// </summary>
    private static async Task ConsumeRowsAsync(
        ChannelReader<object?[]> input,
        IDataWriter writer,
        int batchSize,
        ProgressReporter progress,
        Action<int> updateRowCount,
        CancellationToken ct,
        ILogger logger)
    {
        logger.LogDebug("Consumer/Writer started");
        var buffer = new List<object?[]>(batchSize);

        async Task WriteBufferAsync()
        {
            if (buffer.Count == 0) return;

            logger.LogDebug("Writing batch of {Count} rows", buffer.Count);
            await writer.WriteBatchAsync(buffer, ct);
            logger.LogDebug("Batch written");

            logger.LogDebug("Batch written");

            updateRowCount(buffer.Count);
            progress.ReportWrite(buffer.Count);

            // Trace memory usage
            LogMemoryUsage(logger);

            buffer.Clear();
        }

        await foreach (var row in input.ReadAllAsync(ct))
        {
            buffer.Add(row);

            if (buffer.Count >= batchSize)
            {
                await WriteBufferAsync();
            }
        }

        // Write remaining
        await WriteBufferAsync();
    }

    private static void LogMemoryUsage(ILogger logger)
    {
        if (!logger.IsEnabled(LogLevel.Debug)) return;

        var managedMemory = GC.GetTotalMemory(false) / 1024 / 1024;
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        var totalMemory = process.WorkingSet64 / 1024 / 1024;
        
        // Managed = .NET Objects (GC)
        // WorkingSet = Actual RAM usage (includes Native/Unmanaged drivers like DuckDB/Oracle)
        logger.LogDebug("Memory Stats: Managed={Managed}MB, WorkingSet={Total}MB", managedMemory, totalMemory);
    }
}
