using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.Generate;
using DtPipe.Core;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Data.Analysis;
using Microsoft.Extensions.Logging;

namespace DtPipe.Sample;

/// <summary>
/// This sample demonstrates how to use the DtPipe.Core and DtPipe.Adapters
/// libraries programmatically inside your own .NET applications.
/// </summary>
class Program
{
    static async Task Main(string[] args)
    {
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });

        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("Starting DtPipe SDK Samples...\n");

        await RunFullPipelineExampleAsync(loggerFactory);
        Console.WriteLine("\n--------------------------------------------------\n");
        await RunReaderOnlyExampleAsync(loggerFactory);
        Console.WriteLine("\n--------------------------------------------------\n");
        await RunWriterOnlyExampleAsync(loggerFactory);
        Console.WriteLine("\n--------------------------------------------------\n");
        await RunDataFrameToWriterExampleAsync(loggerFactory);
        Console.WriteLine("\n--------------------------------------------------\n");
        await RunCustomTransformerExampleAsync(loggerFactory);
        Console.WriteLine("\n--------------------------------------------------\n");
        await RunLinqToStreamReaderExampleAsync(loggerFactory);
    }

    /// <summary>
    /// Scenario 1: Using the PipelineEngine to bridge a Reader and a Writer.
    /// This is the standard, optimized DtPipe execution model.
    /// </summary>
    static async Task RunFullPipelineExampleAsync(ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("--- Scenario 1: Full Pipeline Engine ---");

        var engine = new PipelineEngine(loggerFactory.CreateLogger<PipelineEngine>());

        var readerOptions = new GenerateReaderOptions { RowCount = 5 };
        var reader = new GenerateReader("generate:5", "", readerOptions);

        var writerOptions = new CsvWriterOptions { Separator = ";", Header = true };
        var writer = new CsvDataWriter("-", writerOptions); // "-" means STDOUT

        await reader.OpenAsync(CancellationToken.None);
        await writer.InitializeAsync(reader.Columns!, CancellationToken.None);

        long rowCount = await engine.RunAsync(
            reader: reader,
            writer: writer,
            pipeline: null,
            batchSize: 5,
            ct: CancellationToken.None
        );

        logger.LogInformation("Pipeline completed successfully! Transferred {RowCount} rows.", rowCount);
    }

    /// <summary>
    /// Scenario 2: Consuming a DtPipe Reader manually.
    /// Useful if you just want to extract data using DtPipe's optimized adapters
    /// and process it with your own custom logic.
    /// </summary>
    static async Task RunReaderOnlyExampleAsync(ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("--- Scenario 2: Reader Only (Manual Consumption) ---");

        var readerOptions = new GenerateReaderOptions { RowCount = 3 };
        var reader = new GenerateReader("generate:3", "", readerOptions);

        await reader.OpenAsync(CancellationToken.None);
        logger.LogInformation("Schema contains {Count} columns:", reader.Columns!.Count);
        foreach (var col in reader.Columns)
        {
            logger.LogInformation(" - {Name} ({Type})", col.Name, col.ClrType.Name);
        }

        long rowCount = 0;
        // ReadBatchesAsync returns IAsyncEnumerable<ReadOnlyMemory<object?[]>>
        await foreach (var batchChunk in reader.ReadBatchesAsync(batchSize: 2, CancellationToken.None))
        {
            var span = batchChunk.Span;
            for (int i = 0; i < span.Length; i++)
            {
                var row = span[i];
                logger.LogInformation("Manual row read: [{Values}]", string.Join(", ", row));
                rowCount++;
            }
        }

        logger.LogInformation("Reader extraction completed! Read {RowCount} rows.", rowCount);
    }

    /// <summary>
    /// Scenario 3: Pushing data to a DtPipe Writer manually.
    /// Useful if you already have data in memory and want to leverage DtPipe's
    /// optimized bulk inserters (e.g., PostgreSQL COPY, Oracle Array Binding).
    /// </summary>
    static async Task RunWriterOnlyExampleAsync(ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("--- Scenario 3: Writer Only (Manual Push) ---");

        var writerOptions = new CsvWriterOptions { Separator = ",", Header = true };
        var writer = new CsvDataWriter("-", writerOptions);

        // Define the schema artificially
        var columns = new[]
        {
            new PipeColumnInfo("Id", typeof(long), IsNullable: false),
            new PipeColumnInfo("Name", typeof(string), IsNullable: true)
        };

        await writer.InitializeAsync(columns, CancellationToken.None);

        // Create a batch of data
        var batch1 = new object?[][]
        {
            new object?[] { 101L, "Alice" },
            new object?[] { 102L, "Bob" }
        };

        var batch2 = new object?[][]
        {
            new object?[] { 103L, "Charlie" }
        };

        logger.LogInformation("Writing batches...");
        await writer.WriteBatchAsync(batch1, CancellationToken.None);
        await writer.WriteBatchAsync(batch2, CancellationToken.None);

        // Crucial: Complete the writer to flush buffers/footers
        await writer.CompleteAsync(CancellationToken.None);

        logger.LogInformation("Writer completion signal sent.");
    }

    /// <summary>
    /// Scenario 4: Exporting a Microsoft.Data.Analysis DataFrame into a DtPipe writer.
    /// Native integration mapping DataFrame columns to PipeColumnInfo schemas.
    /// </summary>
    static async Task RunDataFrameToWriterExampleAsync(ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("--- Scenario 4: DataFrame to Writer ---");

        // 1. Create a dummy DataFrame
        var df = new DataFrame(
            new PrimitiveDataFrameColumn<int>("UserId", new[] { 1, 2, 3 }),
            new StringDataFrameColumn("Email", new[] { "a@test.com", "b@test.com", "c@test.com" }),
            new PrimitiveDataFrameColumn<bool>("IsActive", new[] { true, false, true })
        );

        // 2. Discover Schema dynamically
        var columns = new PipeColumnInfo[df.Columns.Count];
        for (int i = 0; i < df.Columns.Count; i++)
        {
            var col = df.Columns[i];
            columns[i] = new PipeColumnInfo(col.Name, col.DataType, IsNullable: true);
        }

        // 3. Setup Writer
        var writerOptions = new CsvWriterOptions { Separator = ",", Header = true };
        var writer = new CsvDataWriter("-", writerOptions); // STDOUT

        await writer.InitializeAsync(columns, CancellationToken.None);

        // 4. Translate and Push
        var batch = new object?[df.Rows.Count][];
        for (long i = 0; i < df.Rows.Count; i++)
        {
            var row = new object?[df.Columns.Count];
            for (int c = 0; c < df.Columns.Count; c++)
            {
                row[c] = df.Columns[c][i];
            }
            batch[i] = row;
        }

        logger.LogInformation("Writing DataFrame (Rows: {Rows}, Cols: {Cols})...", df.Rows.Count, df.Columns.Count);

        await writer.WriteBatchAsync(batch, CancellationToken.None);
        await writer.CompleteAsync(CancellationToken.None);

        logger.LogInformation("DataFrame export completed.");
    }

    /// <summary>
    /// Scenario 5: Building and injecting a custom compiled C# Transformer.
    /// This pattern is useful when Javascript masking/computing is not fast enough
    /// or when you need access to complex .NET libraries during the pipeline transfer.
    /// </summary>
    static async Task RunCustomTransformerExampleAsync(ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("--- Scenario 5: Custom C# Transformer ---");

        var engine = new PipelineEngine(loggerFactory.CreateLogger<PipelineEngine>());

        // Source: 3 Fake rows
        var reader = new GenerateReader("generate:3", "", new GenerateReaderOptions { RowCount = 3 });
        await reader.OpenAsync(CancellationToken.None);

        // Transformer: Mutate the data in flight
        var myCustomTransformer = new SetupGreetingTransformer();

        // Writer: Output to STDIN
        var writer = new CsvDataWriter("-", new CsvWriterOptions { Separator = ",", Header = true });

        // The transformer must be able to mutate the schema if needed: Initialize it with reader.Columns
        var modifiedSchema = await myCustomTransformer.InitializeAsync(reader.Columns!, CancellationToken.None);
        await writer.InitializeAsync(modifiedSchema, CancellationToken.None);

        // Execute Pipeline
        logger.LogInformation("Running Pipeline with Custom C# Transformer...");
        await engine.RunAsync(
            reader: reader,
            writer: writer,
            pipeline: new IDataTransformer[] { myCustomTransformer },
            batchSize: 10,
            ct: CancellationToken.None
        );
    }

    /// <summary>
    /// A minimalist transformer that appends "Hello " to the first column (GenerateIndex).
    /// </summary>
    private class SetupGreetingTransformer : IDataTransformer
    {
        public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
        {
            // We return the exact same schema. We could also add Virtual Columns here.
            return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
        }

        public object?[]? Transform(object?[] row)
        {
            // Simple robust transformation using pure C# (compiled natively, very fast)
            if (row.Length > 0 && row[0] is long id)
            {
                row[0] = $"Hello {id}"; // Boxing the new string into the object array
            }
            return row; // Return the mutated array to yield it downstream
        }
    }

    /// <summary>
    /// Scenario 6: Binding any standard .NET LINQ IAsyncEnumerable or IEnumerable
    /// sequence directly into the PipelineEngine as a Native Reader.
    /// </summary>
    static async Task RunLinqToStreamReaderExampleAsync(ILoggerFactory loggerFactory)
    {
        var logger = loggerFactory.CreateLogger<Program>();
        logger.LogInformation("--- Scenario 6: Pure LINQ Object Generator ---");

        var engine = new PipelineEngine(loggerFactory.CreateLogger<PipelineEngine>());

        // 1. We create a generic C# Enumerable using standard code
        var myMemoryDataList = Enumerable.Range(1, 4).Select(i => new { ProductId = i, Name = $"Product_{i}", Price = 10.99m * i });

        // 2. Wrap the IEnumerable into a Custom IStreamReader
        var reader = new LinqStreamReader(myMemoryDataList);
        await reader.OpenAsync(CancellationToken.None);

        // 3. Setup standard CSV Writer to prove native interoperability
        var writer = new CsvDataWriter("-", new CsvWriterOptions { Separator = ";", Header = true });
        await writer.InitializeAsync(reader.Columns!, CancellationToken.None);

        logger.LogInformation("Transferring LINQ sequence to CsvDataWriter...");
        await engine.RunAsync(reader, writer, batchSize: 2, ct: CancellationToken.None);
    }

    /// <summary>
    /// Adapter class wrapping a LINQ Enumerable into DtPipe's high-performance ReadBatchesAsync paradigm.
    /// </summary>
    private class LinqStreamReader : IStreamReader
    {
        private readonly IEnumerable<dynamic> _source;
        public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

        public LinqStreamReader(IEnumerable<dynamic> source) => _source = source;

        public Task OpenAsync(CancellationToken ct = default)
        {
            // Hardcode or reflect the schema
            Columns = new[]
            {
                new PipeColumnInfo("ProductId", typeof(int), IsNullable: false),
                new PipeColumnInfo("Name", typeof(string), IsNullable: false),
                new PipeColumnInfo("Price", typeof(decimal), IsNullable: false)
            };
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            var buffer = new object?[batchSize][];
            int count = 0;

            foreach (var item in _source)
            {
                buffer[count++] = new object?[] { item.ProductId, item.Name, item.Price };
                if (count == batchSize)
                {
                    yield return buffer.AsMemory(0, count);
                    count = 0;
                    buffer = new object?[batchSize][];
                }
            }

            if (count > 0)
            {
                yield return buffer.AsMemory(0, count); // Flush remaining items
            }

            await Task.Yield(); // Satisfy async compiler expectation
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
