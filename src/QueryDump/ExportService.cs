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

        using var progress = new ProgressReporter();
        long totalRows = 0;

        try
        {
            // Run concurrent pipeline: Producer (Reads/Unbatches) -> Transform (Streams rows) -> Consumer (Batches/Writes)
            var producerTask = ProduceRowsAsync(reader, readerToTransform.Writer, options.BatchSize, ct);
            var transformTask = TransformRowsAsync(readerToTransform.Reader, transformToWriter.Writer, pipeline, ct);
            var consumerTask = ConsumeRowsAsync(transformToWriter.Reader, writer, options.BatchSize, progress, r => Interlocked.Add(ref totalRows, r), ct);

            await Task.WhenAll(producerTask, transformTask, consumerTask);

            await writer.CompleteAsync(ct);
            progress.Complete();
            Console.Error.WriteLine($"Export complete: {Interlocked.Read(ref totalRows):N0} rows");
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
        CancellationToken ct)
    {
        try
        {
            await foreach (var batchChunk in reader.ReadBatchesAsync(batchSize, ct))
            {
                for (var i = 0; i < batchChunk.Length; i++)
                {
                    await output.WriteAsync(batchChunk.Span[i], ct);
                }
            }
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
        long cumulativeRows = 0;

        await foreach (var row in input.ReadAllAsync(ct))
        {
            buffer.Add(row);
            
            if (buffer.Count >= batchSize)
            {
                await writer.WriteBatchAsync(buffer, ct);
                cumulativeRows += buffer.Count;
                updateRowCount(buffer.Count);
                progress.Update(cumulativeRows, writer.BytesWritten);
                buffer = new List<object?[]>(batchSize); // New buffer to avoid reference issues
            }
        }

        // Write remaining
        if (buffer.Count > 0)
        {
            await writer.WriteBatchAsync(buffer, ct);
            cumulativeRows += buffer.Count;
            updateRowCount(buffer.Count);
            progress.Update(cumulativeRows, writer.BytesWritten);
        }
    }
}
