using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Columnar.Filter;
using DtPipe.Transformers.Services;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Runtime.CompilerServices;
using Xunit;

namespace DtPipe.Tests.Integration.ZeroCopy;

public class ZeroCopyPipelineTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _parquetPath;
    private readonly string _connectionString;

    public ZeroCopyPipelineTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"zc_test_{Guid.NewGuid()}.duckdb");
        _parquetPath = Path.Combine(Path.GetTempPath(), $"zc_test_{Guid.NewGuid()}.parquet");
        _connectionString = $"Data Source={_dbPath}";
    }

    public async ValueTask InitializeAsync()
    {
        // Seed DuckDB data
        using var connection = new DuckDB.NET.Data.DuckDBConnection(_connectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE source (id INTEGER, val DOUBLE, name VARCHAR)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO source VALUES (1, 10.5, 'Alice'), (2, 20.0, 'Bob'), (3, 30.5, 'Charlie')";
        await cmd.ExecuteNonQueryAsync();
    }

    public ValueTask DisposeAsync()
    {
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_parquetPath); } catch { }
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task DuckDb_To_Parquet_WithFilter_ShouldBe_ZeroCopy()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();

        var registry = new OptionsRegistry();
        services.AddSingleton(registry);

        // Mock Bridge Factories to ensure they are NOT used (setup returns to avoid null ref)
        var rowToColFactory = new Mock<IRowToColumnarBridgeFactory>();
        var colToRowFactory = new Mock<IColumnarToRowBridgeFactory>();

        rowToColFactory.Setup(f => f.CreateBridge())
            .Returns(() => new DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge(NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance));
        colToRowFactory.Setup(f => f.CreateBridge())
            .Returns(() => new DtPipe.Core.Infrastructure.Arrow.ArrowColumnarToRowBridge());

        services.AddSingleton(rowToColFactory.Object);
        services.AddSingleton(colToRowFactory.Object);

        var mockProgress = new Mock<IExportProgress>();
        mockProgress.Setup(p => p.GetMetrics()).Returns(new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0, new Dictionary<string, long>()));
        var mockObserver = new Mock<IExportObserver>();
        mockObserver.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IEnumerable<string>>()))
                    .Returns(mockProgress.Object);
        services.AddSingleton(mockObserver.Object);

        var mockJs = new Mock<IJsEngineProvider>();
        mockJs.Setup(j => j.GetEngine()).Returns(new Jint.Engine());
        services.AddSingleton(mockJs.Object);

        services.AddSingleton<ExportService>();
        var serviceProvider = services.BuildServiceProvider();
        var exportService = serviceProvider.GetRequiredService<ExportService>();

        // Configure Options
        var pipelineOptions = new PipelineOptions { Limit = 100 };
        var readerOptions = new DuckDbReaderOptions { Query = "SELECT * FROM source" };
        var writerOptions = new ParquetWriterOptions();

        registry.RegisterByType(typeof(PipelineOptions), pipelineOptions);
        registry.Register(readerOptions);
        registry.Register(writerOptions);

        // Factories
        var readerFactory = new DuckDataSourceReaderDescriptor();
        var writerFactory = new ParquetWriterDescriptor();

        // Pipeline: Filter (Columnar)
        var filter = new FilterDataTransformer(new FilterTransformerOptions { Filters = new[] { "val > 15" } }, mockJs.Object);
        var pipeline = new List<IDataTransformer> { filter };

        // Act
        await exportService.RunExportAsync(
            pipelineOptions,
            "duckdb",
            _parquetPath,
            CancellationToken.None,
            pipeline,
            new MockReaderFactory(readerFactory, _connectionString, readerOptions, serviceProvider),
            new MockWriterFactory(writerFactory, _parquetPath, writerOptions, serviceProvider),
            registry);

        // Assert
        rowToColFactory.Verify(f => f.CreateBridge(), Times.Never, "Row-to-Columnar bridge should NOT be used in a 100% columnar pipeline.");
        colToRowFactory.Verify(f => f.CreateBridge(), Times.Never, "Columnar-to-Row bridge should NOT be used in a 100% columnar pipeline.");

        // Verify Parquet output correctly contains filtered data
        File.Exists(_parquetPath).Should().BeTrue();
        await using (var reader = new ParquetStreamReader(_parquetPath))
        {
            await reader.OpenAsync();
            var rows = new List<object?[]>();
            await foreach (var batch in reader.ReadBatchesAsync(100))
            {
                rows.AddRange(batch.ToArray());
            }
            rows.Should().HaveCount(2); // Bob (20.0) and Charlie (30.5)
            rows.Any(r => r[2]?.ToString() == "Alice").Should().BeFalse();
        }
    }

    private class MockReaderFactory : IStreamReaderFactory
    {
        private readonly IProviderDescriptor<IStreamReader> _descriptor;
        private readonly string _cs;
        private readonly object _opt;
        private readonly IServiceProvider _sp;

        public MockReaderFactory(IProviderDescriptor<IStreamReader> descriptor, string cs, object opt, IServiceProvider sp)
        {
            _descriptor = descriptor;
            _cs = cs;
            _opt = opt;
            _sp = sp;
        }

        public string ComponentName => _descriptor.ComponentName;
        public string Category => _descriptor.Category;
        public Type OptionsType => _descriptor.OptionsType;
        public bool RequiresQuery => _descriptor.RequiresQuery;
        public bool CanHandle(string connectionString) => _descriptor.CanHandle(connectionString);
        public IStreamReader Create(OptionsRegistry registry) => (IStreamReader)_descriptor.Create(_cs, _opt, _sp);
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { _descriptor.OptionsType };
    }

    private class MockWriterFactory : IDataWriterFactory
    {
        private readonly IProviderDescriptor<IDataWriter> _descriptor;
        private readonly string _path;
        private readonly object _opt;
        private readonly IServiceProvider _sp;

        public MockWriterFactory(IProviderDescriptor<IDataWriter> descriptor, string path, object opt, IServiceProvider sp)
        {
            _descriptor = descriptor;
            _path = path;
            _opt = opt;
            _sp = sp;
        }

        public string ComponentName => _descriptor.ComponentName;
        public string Category => _descriptor.Category;
        public Type OptionsType => _descriptor.OptionsType;
        public bool CanHandle(string path) => _descriptor.CanHandle(path);
        public IDataWriter Create(OptionsRegistry registry) => _descriptor.Create(_path, _opt, _sp);
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { _descriptor.OptionsType };
    }
}
