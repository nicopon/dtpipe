using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Feedback;
using QueryDump.Transformers.Fake;
using QueryDump.Writers;

namespace QueryDump;

public class ExportService
{
    private readonly IDataSourceFactory _dataSourceFactory;
    private readonly IDataWriterFactory _dataWriterFactory;
    private readonly IFakeDataTransformerFactory _fakeDataTransformerFactory;
    private readonly OptionsRegistry _optionsRegistry;

    public ExportService(
        IDataSourceFactory dataSourceFactory, 
        IDataWriterFactory dataWriterFactory,
        IFakeDataTransformerFactory fakeDataTransformerFactory,
        OptionsRegistry optionsRegistry)
    {
        _dataSourceFactory = dataSourceFactory;
        _dataWriterFactory = dataWriterFactory;
        _fakeDataTransformerFactory = fakeDataTransformerFactory;
        _optionsRegistry = optionsRegistry;
    }

    public async Task RunExportAsync(DumpOptions options, CancellationToken ct)
    {
        Console.Error.WriteLine($"Connecting to {options.Provider}...");
        
        await using var reader = _dataSourceFactory.Create(options);
        
        await reader.OpenAsync(ct);
        
        if (reader.Columns is null || reader.Columns.Count == 0)
        {
            Console.Error.WriteLine("No columns returned by query.");
            return;
        }

        Console.Error.WriteLine($"Schema: {reader.Columns.Count} columns");
        Console.Error.WriteLine($"Writing to: {options.OutputPath}");

        // Initialize transformer
        var transformer = _fakeDataTransformerFactory.Create(options);
        if (transformer is not null)
        {
             await transformer.InitializeAsync(reader.Columns, ct);
             // Cannot cast to concrete to check property easily unless we cast or expose 'HasMappings' on IDataTransformer? 
             // But InitializeAsync generally handles "no-op".
             // We'll log based on options for now.
             var fakeOptions = _optionsRegistry.Get<FakeOptions>();
             Console.Error.WriteLine($"Fake data: {fakeOptions.Mappings.Count} column(s) masked");
        }

        await using var writer = _dataWriterFactory.Create(options);
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

                // Transformation pipeline
                if (transformer != null)
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
