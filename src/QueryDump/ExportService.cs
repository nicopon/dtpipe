using System.Threading.Channels;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Feedback;
using Spectre.Console;

namespace QueryDump;

public class ExportService
{
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;
    private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
    private readonly OptionsRegistry _optionsRegistry;
    private readonly IAnsiConsole _console;

    public ExportService(
        IEnumerable<IStreamReaderFactory> readerFactories, 
        IEnumerable<IDataWriterFactory> writerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        OptionsRegistry optionsRegistry,
        IAnsiConsole console)
    {
        _readerFactories = readerFactories;
        _writerFactories = writerFactories;
        _transformerFactories = transformerFactories;
        _optionsRegistry = optionsRegistry;
        _console = console;
    }

    public async Task RunExportAsync(DumpOptions options, CancellationToken ct, List<IDataTransformer> pipeline)
    {
        _console.MarkupLine($"[grey]Connecting to provider:[/] [blue]{options.Provider}[/]");
        
        // Resolve reader factory
        var readerFactory = _readerFactories.FirstOrDefault(f => 
            f.ProviderName.Equals(options.Provider, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Unknown provider: {options.Provider}. Supported: {string.Join(", ", _readerFactories.Select(f => f.ProviderName))}");
        
        await using var reader = readerFactory.Create(options);
        
        await reader.OpenAsync(ct);
        
        if (reader.Columns is null || reader.Columns.Count == 0)
        {
            _console.MarkupLine("[red]No columns returned by query.[/]");
            return;
        }

        // Initialize pipeline to get Target Schema
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

        // Dry-run mode: display unified analysis
        if (options.DryRun)
        {
            _console.WriteLine();
            _console.MarkupLine("[grey]Fetching sample row for analysis...[/]");
            
            object?[]? inputRow = null;
            object?[]? outputRow = null;

            // Fetch one row
            await foreach(var batch in reader.ReadBatchesAsync(1, ct))
            {
                if (batch.Length > 0)
                {
                    var span = batch.Span[0];
                    inputRow = span.ToArray(); // Capture input
                    var rowToTransform = span.ToArray(); // Copy for transform
                    
                    // Transform
                    foreach (var t in pipeline)
                    {
                        rowToTransform = t.Transform(rowToTransform);
                    }
                    outputRow = rowToTransform;
                }
                break;
            }

            _console.WriteLine();
            _console.Write(new Rule("[green]Dry-Run Analysis[/]").LeftJustified());

            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Column");
            table.AddColumn("Input Type");
            table.AddColumn("Input Value");
            table.AddColumn("Output Type");
            table.AddColumn("Output Value");

            for (int i = 0; i < currentSchema.Count; i++)
            {
                var col = currentSchema[i];
                var nameDisplay = col.IsVirtual ? $"{col.Name} [grey](virtual)[/]" : col.Name;

                // Input Info
                string inputType = "";
                string inputValue = "";
                
                // Find column in reader to get Input details
                var readerColIndex = -1;
                for (int k = 0; k < reader.Columns.Count; k++)
                {
                    if (reader.Columns[k].Name == col.Name)
                    {
                        readerColIndex = k;
                        break;
                    }
                }

                if (readerColIndex >= 0)
                {
                    inputType = $"[grey]{reader.Columns[readerColIndex].ClrType.Name}[/]";
                    if (inputRow != null && readerColIndex < inputRow.Length)
                    {
                        var val = inputRow[readerColIndex];
                        inputValue = val is null ? "[grey]null[/]" : Markup.Escape(val.ToString() ?? "");
                    }
                }

                // Output Info
                var outputType = $"[blue]{col.ClrType.Name}[/]";
                var outputValue = "";
                if (outputRow != null && i < outputRow.Length)
                {
                    var val = outputRow[i];
                    var valStr = val is null ? "[grey]null[/]" : Markup.Escape(val.ToString() ?? "");
                    
                    // Highlight difference
                    if (valStr != inputValue && inputValue != "")
                        outputValue = $"[yellow]{valStr}[/]";
                    else
                        outputValue = valStr;
                }

                table.AddRow(
                    nameDisplay,
                    inputType,
                    inputValue,
                    outputType,
                    outputValue
                );
            }
            _console.Write(table);
            
            _console.WriteLine();
            _console.MarkupLine("[green]Dry-run complete. No data exported.[/]");
            return;
        }

        _console.MarkupLine($"[grey]Writing to:[/] [blue]{options.OutputPath}[/]");

        // Resolve writer factory
        var extension = Path.GetExtension(options.OutputPath).ToLowerInvariant();
        var writerFactory = _writerFactories.FirstOrDefault(f =>
            f.SupportedExtension.Equals(extension, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Unsupported file format: {extension}. Supported: {string.Join(", ", _writerFactories.Select(f => f.SupportedExtension))}");

        // Writer receives only non-virtual columns (for output)
        var exportableSchema = currentSchema.Where(c => !c.IsVirtual).ToList();
        await using var writer = writerFactory.Create(options);
        await writer.InitializeAsync(exportableSchema, ct);

        // Create bounded channels for pipeline with backpressure
        // Capacity 1000 rows allows for some buffering but keeps backpressure active
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

        using var progress = new ProgressReporter(true, pipeline);
        long totalRows = 0;

        // Create a linked token source for limit-based cancellation
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var effectiveCt = linkedCts.Token;

        try
        {
            // Run concurrent pipeline: Producer (Reads/Unbatches) -> Transform (Streams rows) -> Consumer (Batches/Writes)
            var producerTask = ProduceRowsAsync(reader, readerToTransform.Writer, options.BatchSize, options.Limit, progress, linkedCts, effectiveCt);
            var transformTask = TransformRowsAsync(readerToTransform.Reader, transformToWriter.Writer, pipeline, progress, effectiveCt);
            var consumerTask = ConsumeRowsAsync(transformToWriter.Reader, writer, options.BatchSize, progress, r => Interlocked.Add(ref totalRows, r), effectiveCt);

            await Task.WhenAll(producerTask, transformTask, consumerTask);

            await writer.CompleteAsync(ct);
            progress.Complete();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Export failed: {ex.Message}");
            throw;
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
        ProgressReporter progress,
        CancellationTokenSource linkedCts,
        CancellationToken ct)
    {
        long rowCount = 0;
        try
        {
            await foreach (var batchChunk in reader.ReadBatchesAsync(batchSize, ct))
            {
                for (var i = 0; i < batchChunk.Length; i++)
                {
                    await output.WriteAsync(batchChunk.Span[i], ct);
                    progress.ReportRead(1);
                    rowCount++;

                    // Check limit and cancel if reached
                    if (limit > 0 && rowCount >= limit)
                    {
                        await linkedCts.CancelAsync();
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
        CancellationToken ct)
    {
        var buffer = new List<object?[]>(batchSize);
        long previousBytes = 0;

        await foreach (var row in input.ReadAllAsync(ct))
        {
            buffer.Add(row);
            
            if (buffer.Count >= batchSize)
            {
                await writer.WriteBatchAsync(buffer, ct);
                
                var currentBytes = writer.BytesWritten;
                var bytesDelta = currentBytes - previousBytes;
                previousBytes = currentBytes;

                updateRowCount(buffer.Count); // Update total
                progress.ReportWrite(buffer.Count, bytesDelta);
                
                buffer.Clear();
            }
        }

        // Write remaining
        if (buffer.Count > 0)
        {
            await writer.WriteBatchAsync(buffer, ct);
            
            var currentBytes = writer.BytesWritten;
            var bytesDelta = currentBytes - previousBytes;
            
            updateRowCount(buffer.Count);
            progress.ReportWrite(buffer.Count, bytesDelta);
        }
    }
}
