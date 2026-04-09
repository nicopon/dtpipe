using Apache.Arrow;
using DtPipe.Adapters.Infrastructure.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Arrow;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Arrow.Format;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Services;
using DtPipe.Transformers.Services;
using Apache.Arrow.Types;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DtPipe.Tests.Integration;

public class ExportServiceIntegrationTests
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ExportService _exportService;

    public ExportServiceIntegrationTests()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton<IRowToColumnarBridgeFactory, ArrowRowToColumnarBridgeFactory>();
        services.AddSingleton<IColumnarToRowBridgeFactory, ArrowColumnarToRowBridgeFactory>();

        var mockProgress = new Mock<IExportProgress>();
        mockProgress.Setup(p => p.GetMetrics()).Returns(new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0, new Dictionary<string, long>()));
        var mockObserver = new Mock<IExportObserver>();
        mockObserver.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IReadOnlyList<(string Name, bool IsColumnar)>>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()))
                    .Returns(mockProgress.Object);
        services.AddSingleton(mockObserver.Object);
        services.AddSingleton<HookExecutor>();
        services.AddSingleton<MetricsService>();
        services.AddSingleton<SchemaValidationService>();
        services.AddSingleton<PipelineExecutor>();

        services.AddSingleton<ExportService>();

        var mockJs = new Mock<IJsEngineProvider>();
        mockJs.Setup(j => j.GetEngine()).Returns(new Jint.Engine());
        services.AddSingleton(mockJs.Object);

        _serviceProvider = services.BuildServiceProvider();
        _exportService = _serviceProvider.GetRequiredService<ExportService>();
    }

    [Fact]
    public async Task RunExportAsync_WithMixedPipeline_ShouldBridgeCorrectly()
    {
        // Setup: Columnar Reader -> Columnar Filter -> Row Compute -> Columnar Format -> Columnar Writer
        // This exercises COL -> ROW and ROW -> COL bridging.

        var columns = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Val", typeof(int), false)
        };

        var data = new List<object?[]>
        {
            new object?[] { 1, "Alpha", 10 },
            new object?[] { 2, "Beta", 20 },
            new object?[] { 3, "Gamma", 30 }
        };

        var readerMock = new Mock<IColumnarStreamReader>();
        readerMock.SetupGet(r => r.Columns).Returns(columns);

        // Mocking ReadRecordBatchesAsync is tricky, better use a real simple implementation if possible
        // but for now let's use a helper to create a RecordBatch
        var batch = CreateBatch(columns, data);
        readerMock.Setup(r => r.ReadRecordBatchesAsync(It.IsAny<CancellationToken>()))
                  .Returns(AsyncEnumerable(batch));

        var writerMock = new Mock<IColumnarDataWriter>();
        var capturedBatches = new List<RecordBatch>();
        writerMock.Setup(w => w.WriteRecordBatchAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()))
                  .Callback<RecordBatch, CancellationToken>((b, ct) => capturedBatches.Add(b))
                  .Returns(new ValueTask(Task.CompletedTask));

        // Pipeline:
        // 1. Filter (Columnar): Val > 15 (Drops Id 1)
        var jsProvider = _serviceProvider.GetRequiredService<IJsEngineProvider>();
        var filter = new FilterDataTransformer(new FilterOptions { Filters = new[] { "Val > 15" } }, jsProvider);
        await filter.InitializeAsync(columns);

        // 2. Compute (Row): NewVal = Val * 2
        var compute = new ComputeDataTransformer(new ComputeOptions { Compute = new[] { "NewVal:int:row.Val * 2" } }, jsProvider);
        var midSchema = await compute.InitializeAsync(columns);

        // 3. Format (Columnar): Name = "[{Name}]"
        var format = new FormatDataTransformer(new FormatOptions { Format = new[] { "Name:[{Name}]" } });
        var finalSchema = await format.InitializeAsync(midSchema);

        var pipeline = new List<IDataTransformer> { filter, compute, format };

        var options = new PipelineOptions { BatchSize = 100 };
        var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();

        var readerFactory = new Mock<IStreamReaderFactory>();
        readerFactory.Setup(f => f.Create(It.IsAny<OptionsRegistry>())).Returns(readerMock.Object);

        var writerFactory = new Mock<IDataWriterFactory>();
        writerFactory.Setup(f => f.Create(It.IsAny<OptionsRegistry>())).Returns(writerMock.Object);

        // Act
        await _exportService.RunExportAsync(options, "mock", "out", CancellationToken.None, pipeline, readerFactory.Object, writerFactory.Object, registry);

        // Assert
        capturedBatches.Should().HaveCount(1);
        var resultBatch = capturedBatches[0];
        resultBatch.Length.Should().Be(2); // Alpha 10 filtered out

        // Check contents (Columnar bridges and segments worked)
        var nameCol = (StringArray)resultBatch.Column(1);
        nameCol.GetString(0).Should().Be("[Beta]");
        nameCol.GetString(1).Should().Be("[Gamma]");

        var newValCol = (Int32Array)resultBatch.Column(3); // Id, Name, Val, NewVal
        newValCol.GetValue(0).Should().Be(40);
        newValCol.GetValue(1).Should().Be(60);
    }

    private RecordBatch CreateBatch(IReadOnlyList<PipeColumnInfo> columns, List<object?[]> data)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("Id").DataType(Int32Type.Default))
            .Field(f => f.Name("Name").DataType(StringType.Default))
            .Field(f => f.Name("Val").DataType(Int32Type.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var valBuilder = new Int32Array.Builder();

        foreach (var row in data)
        {
            idBuilder.Append((int)row[0]!);
            nameBuilder.Append((string)row[1]!);
            valBuilder.Append((int)row[2]!);
        }

        return new RecordBatch(schema, new IArrowArray[]
        {
            idBuilder.Build(),
            nameBuilder.Build(),
            valBuilder.Build()
        }, data.Count);
    }

    private async IAsyncEnumerable<T> AsyncEnumerable<T>(params T[] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        await Task.Yield(); // Just to make it async
    }
}
