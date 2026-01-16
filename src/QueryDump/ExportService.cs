using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Feedback;
using QueryDump.Writers;

namespace QueryDump;

public class ExportService
{
    private readonly IEnumerable<IReaderFactory> _readerFactories;
    private readonly IEnumerable<IWriterFactory> _writerFactories;
    private readonly IEnumerable<ITransformerFactory> _transformerFactories;
    private readonly OptionsRegistry _optionsRegistry;

    public ExportService(
        IEnumerable<IReaderFactory> readerFactories, 
        IEnumerable<IWriterFactory> writerFactories,
        IEnumerable<ITransformerFactory> transformerFactories,
        OptionsRegistry optionsRegistry)
    {
        _readerFactories = readerFactories;
        _writerFactories = writerFactories;
        _transformerFactories = transformerFactories;
        _optionsRegistry = optionsRegistry;
    }

    public async Task RunExportAsync(DumpOptions options, CancellationToken ct)
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

        // Initialize transformation pipeline
        var pipeline = new List<IDataTransformer>();
        foreach (var factory in _transformerFactories)
        {
            var transformer = factory.Create(options);
            if (transformer != null)
            {
                pipeline.Add(transformer);
            }
        }
        
        // Sort by Priority (Low -> High)
        pipeline.Sort((a, b) => a.Priority.CompareTo(b.Priority));

        if (pipeline.Count > 0)
        {
             Console.Error.WriteLine($"Transformation Pipeline: {string.Join(" -> ", pipeline.Select(p => $"{p.GetType().Name} (Pr:{p.Priority})"))}");
             foreach (var t in pipeline)
             {
                 await t.InitializeAsync(reader.Columns, ct);
             }
        }

        // Resolve writer factory
        var extension = Path.GetExtension(options.OutputPath).ToLowerInvariant();
        var writerFactory = _writerFactories.FirstOrDefault(f =>
            f.SupportedExtension.Equals(extension, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Unsupported file format: {extension}. Supported: {string.Join(", ", _writerFactories.Select(f => f.SupportedExtension))}");

        await using var writer = writerFactory.Create(options);
        await writer.InitializeAsync(reader.Columns, ct);

        using var progress = new ProgressReporter();
        
        long totalRows = 0;

        try 
        {
            await foreach (var batchChunk in reader.ReadBatchesAsync(options.BatchSize, ct))
            {
                var rows = new List<object?[]>(batchChunk.Length);
                for (var i = 0; i < batchChunk.Length; i++)
                {
                    rows.Add(batchChunk.Span[i]);
                }

                IReadOnlyList<object?[]> processedBatch = rows;

                // Execute pipeline
                foreach (var transformer in pipeline)
                {
                    processedBatch = await transformer.TransformAsync(processedBatch, ct);
                }
                
                await writer.WriteBatchAsync(processedBatch, ct);
                totalRows += processedBatch.Count;
                progress.Update(totalRows, writer.BytesWritten);
            }

            await writer.CompleteAsync(ct);
            progress.Complete();
            Console.Error.WriteLine($"Export complete: {totalRows:N0} rows");
        }
        catch (Exception ex)
        {
             Console.Error.WriteLine($"Export failed: {ex.Message}");
             throw;
        }
    }
}
