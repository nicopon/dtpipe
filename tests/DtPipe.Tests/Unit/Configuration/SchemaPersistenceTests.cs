using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.JsonL;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Engine;
using DtPipe.Services;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Configuration;

public sealed class SchemaPersistenceTests : IDisposable
{
    private readonly string _schemaName = "test_schema_" + Guid.NewGuid().ToString("N");
    private readonly string _schemaFilePath;

    public SchemaPersistenceTests()
    {
        _schemaFilePath = Path.Combine(".dtpipe", "schemas", $"{_schemaName}.dtschema");
    }

    public void Dispose()
    {
        if (File.Exists(_schemaFilePath))
            File.Delete(_schemaFilePath);
    }

    private sealed class ColumnarStubReader : IColumnarStreamReader
    {
        public Schema Schema { get; } = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("user").DataType(new StructType(new[]
            {
                new Field("name", StringType.Default, true),
                new Field("age", Int32Type.Default, true)
            })).Nullable(true))
            .Build();

        public IReadOnlyList<PipeColumnInfo>? Columns =>
            Schema.FieldsList.Select(f => new PipeColumnInfo(f.Name, typeof(object), f.IsNullable)).ToList();

        public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

        public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }

        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ColumnarStubReaderFactory : IStreamReaderFactory
    {
        public string ComponentName => "jsonl";
        public string Category => "Test";
        public Type OptionsType => typeof(JsonLReaderOptions);
        public bool CanHandle(string cs) => false;
        public bool RequiresQuery => false;
        public IEnumerable<Type> GetSupportedOptionTypes() => [OptionsType];
        public IStreamReader Create(OptionsRegistry registry) => new ColumnarStubReader();
    }

    private sealed class StubWriterFactory : IDataWriterFactory
    {
        public string ComponentName => "stub";
        public string Category => "Test";
        public Type OptionsType => typeof(EmptyOptions);
        public bool CanHandle(string cs) => false;
        public IEnumerable<Type> GetSupportedOptionTypes() => [OptionsType];
        public IDataWriter Create(OptionsRegistry registry) => new StubWriter();
    }

    private sealed class StubWriter : IColumnarDataWriter
    {
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
        {
            batch.Dispose();
            return ValueTask.CompletedTask;
        }
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static ExportService BuildExportService(OptionsRegistry registry, IStreamReaderFactory readerFactory, IDataWriterFactory writerFactory)
    {
        var observer = new Mock<IExportObserver>();
        var progress = new Mock<IExportProgress>();
        progress.Setup(p => p.GetMetrics()).Returns(new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0, new Dictionary<string, long>()));
        observer.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IReadOnlyList<(string Name, bool IsColumnar)>>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()))
            .Returns(progress.Object);

        return new ExportService(
            [readerFactory],
            [writerFactory],
            [],
            registry,
            observer.Object,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ExportService>.Instance,
            new HookExecutor(observer.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<HookExecutor>.Instance),
            new MetricsService(observer.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<MetricsService>.Instance),
            new SchemaValidationService(observer.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<SchemaValidationService>.Instance),
            new PipelineExecutor([], [], Microsoft.Extensions.Logging.Abstractions.NullLogger<PipelineExecutor>.Instance));
    }

    [Fact]
    public async Task SchemaSave_Writes_Dtschema_File_After_Reader_Opens()
    {
        var registry = new OptionsRegistry();
        var jsonlOptions = new JsonLReaderOptions { SchemaSave = _schemaName };
        registry.Register(jsonlOptions);

        var readerFactory = new ColumnarStubReaderFactory();
        var writerFactory = new StubWriterFactory();
        var svc = BuildExportService(registry, readerFactory, writerFactory);

        await svc.RunExportAsync(
            new PipelineOptions { NoStats = true },
            "jsonl",
            "stub:out",
            CancellationToken.None,
            [],
            readerFactory,
            writerFactory,
            registry);

        Assert.True(File.Exists(_schemaFilePath), $"Schema file should exist at {_schemaFilePath}");

        var loaded = SchemaStore.Load(_schemaName);
        Assert.NotNull(loaded);
        Assert.Equal(2, loaded.FieldsList.Count);
        Assert.Equal("id", loaded.FieldsList[0].Name);
        Assert.Equal("user", loaded.FieldsList[1].Name);
        Assert.IsType<StructType>(loaded.FieldsList[1].DataType);
    }

    [Fact]
    public async Task SchemaLoad_Missing_File_Throws_FileNotFoundException()
    {
        var registry = new OptionsRegistry();
        var missingName = "nonexistent_schema_" + Guid.NewGuid().ToString("N");
        var jsonlOptions = new JsonLReaderOptions { SchemaLoad = missingName };
        registry.Register(jsonlOptions);

        var readerFactory = new ColumnarStubReaderFactory();
        var writerFactory = new StubWriterFactory();
        var svc = BuildExportService(registry, readerFactory, writerFactory);

        var ex = await Assert.ThrowsAsync<FileNotFoundException>(() => svc.RunExportAsync(
            new PipelineOptions { NoStats = true },
            "jsonl",
            "stub:out",
            CancellationToken.None,
            [],
            readerFactory,
            writerFactory,
            registry));

        Assert.Contains(missingName, ex.Message);
    }

    private sealed class RowStubReader : IStreamReader
    {
        public IReadOnlyList<PipeColumnInfo>? Columns =>
        [
            new("title", typeof(string), true),
            new("count", typeof(int), false)
        ];

        public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RowStubReaderFactory : IStreamReaderFactory
    {
        public string ComponentName => "csv";
        public string Category => "Test";
        public Type OptionsType => typeof(CsvReaderOptions);
        public bool CanHandle(string cs) => false;
        public bool RequiresQuery => false;
        public IEnumerable<Type> GetSupportedOptionTypes() => [OptionsType];
        public IStreamReader Create(OptionsRegistry registry) => new RowStubReader();
    }

    private sealed class StubRowWriterFactory : IDataWriterFactory
    {
        public string ComponentName => "stub-row";
        public string Category => "Test";
        public Type OptionsType => typeof(EmptyOptions);
        public bool CanHandle(string cs) => false;
        public IEnumerable<Type> GetSupportedOptionTypes() => [OptionsType];
        public IDataWriter Create(OptionsRegistry registry) => new StubRowWriter();
    }

    private sealed class StubRowWriter : IRowDataWriter
    {
        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task SchemaSave_RowReader_Saves_Schema_From_PipeColumnInfo()
    {
        var rowSchemaName = _schemaName + "_row";
        var rowSchemaPath = Path.Combine(".dtpipe", "schemas", $"{rowSchemaName}.dtschema");
        try
        {
            var registry = new OptionsRegistry();
            var csvOptions = new CsvReaderOptions { SchemaSave = rowSchemaName };
            registry.Register(csvOptions);

            var readerFactory = new RowStubReaderFactory();
            var writerFactory = new StubRowWriterFactory();
            var svc = BuildExportService(registry, readerFactory, writerFactory);

            await svc.RunExportAsync(
                new PipelineOptions { NoStats = true },
                "csv",
                "stub:out",
                CancellationToken.None,
                [],
                readerFactory,
                writerFactory,
                registry);

            Assert.True(File.Exists(rowSchemaPath), $"Schema file should exist at {rowSchemaPath}");

            var loaded = SchemaStore.Load(rowSchemaName);
            Assert.NotNull(loaded);
            Assert.Equal(2, loaded.FieldsList.Count);
            Assert.Equal("title", loaded.FieldsList[0].Name);
            Assert.Equal("count", loaded.FieldsList[1].Name);
        }
        finally
        {
            if (File.Exists(rowSchemaPath)) File.Delete(rowSchemaPath);
        }
    }

    [Fact]
    public async Task SchemaLoad_With_Dtschema_Extension_Loads_Correctly()
    {
        var registrySave = new OptionsRegistry();
        var jsonlOptionsSave = new JsonLReaderOptions { SchemaSave = _schemaName };
        registrySave.Register(jsonlOptionsSave);

        var readerFactory = new ColumnarStubReaderFactory();
        var writerFactory = new StubWriterFactory();
        var svc = BuildExportService(registrySave, readerFactory, writerFactory);

        await svc.RunExportAsync(
            new PipelineOptions { NoStats = true },
            "jsonl",
            "stub:out",
            CancellationToken.None,
            [],
            readerFactory,
            writerFactory,
            registrySave);

        var registryLoad = new OptionsRegistry();
        var jsonlOptionsLoad = new JsonLReaderOptions { SchemaLoad = $"{_schemaName}.dtschema" };
        registryLoad.Register(jsonlOptionsLoad);

        var loadedReaderFactory = new ColumnarStubReaderFactory();
        var svcLoad = BuildExportService(registryLoad, loadedReaderFactory, writerFactory);

        await svcLoad.RunExportAsync(
            new PipelineOptions { NoStats = true },
            "jsonl",
            "stub:out",
            CancellationToken.None,
            [],
            loadedReaderFactory,
            writerFactory,
            registryLoad);

        Assert.NotEmpty(registryLoad.Get<JsonLReaderOptions>().Schema);
    }
}
