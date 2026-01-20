using System.Threading.Channels;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Feedback;
using QueryDump.Writers;

namespace QueryDump;

public class ExportService
{
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;
    private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
    private readonly OptionsRegistry _optionsRegistry;

    public ExportService(
        IEnumerable<IStreamReaderFactory> readerFactories, 
        IEnumerable<IDataWriterFactory> writerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        OptionsRegistry optionsRegistry)
    {
        _readerFactories = readerFactories;
        _writerFactories = writerFactories;
        _transformerFactories = transformerFactories;
        _optionsRegistry = optionsRegistry;
    }

    public async Task RunExportAsync(DumpOptions options, CancellationToken ct, string[]? args = null)
    {
        Console.Error.WriteLine($"Connecting to {options.Provider}...");
        
        // Resolve reader factory
        var readerFactory = _readerFactories.FirstOrDefault(f => 
            f.ProviderName.Equals(options.Provider, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Unknown provider: {options.Provider}. Supported: {string.Join(", ", _readerFactories.Select(f => f.ProviderName))}");
        
        await using var reader = readerFactory.Create(options);
        
        await reader.OpenAsync(ct);
        
        if (reader.Columns is null || reader.Columns.Count == 0)
        {
            Console.Error.WriteLine("No columns returned by query.");
            return;
        }

        Console.Error.WriteLine($"Schema: {reader.Columns.Count} columns");
        
        // Display column details for dry-run or verbose mode
        foreach (var col in reader.Columns)
        {
            Console.Error.WriteLine($"  - {col.Name}: {col.ClrType.Name}{(col.IsNullable ? "?" : "")}");
        }

        // Dry-run mode: exit after displaying schema
        if (options.DryRun)
        {
            Console.Error.WriteLine("\n[Dry-run mode] Schema displayed. No data exported.");
            return;
        }

        Console.Error.WriteLine($"Writing to: {options.OutputPath}");

        // Initialize transformation pipeline using ordered arguments
        // This ensures the pipeline executes in the exact order specified by the user in CLI
        var pipelineBuilder = new TransformerPipelineBuilder(_transformerFactories);
        var pipeline = pipelineBuilder.Build(args ?? Environment.GetCommandLineArgs());
        
        // If no pipeline was built (e.g. no args matching transformers), pipeline is empty.
        // Legacy fallback: if pipeline is empty, we could check registry, but for now we follow the "ordered" paradigm strictness.
        // Actually, for backward compatibility or ease of use, if the new builder yields nothing but the registry has options,
        // we might want to fallback, but the requirement was "no compatibility" and "order by args".
        // However, we must ensure we don't accidentally pick up "dotnet exec dll" args if not careful, 
        // but the builder logic maps known options.


        // Cascade initialization: output of one transformer becomes input to next
        var currentSchema = reader.Columns;
        if (pipeline.Count > 0)
        {
             Console.Error.WriteLine($"Transformation Pipeline: {string.Join(" -> ", pipeline.Select(p => $"{p.GetType().Name} (Pr:{p.Priority})"))}");
             foreach (var t in pipeline)
             {
                 currentSchema = await t.InitializeAsync(currentSchema, ct);
             }
        }

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
                
                buffer = new List<object?[]>(batchSize); // New buffer to avoid reference issues
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
