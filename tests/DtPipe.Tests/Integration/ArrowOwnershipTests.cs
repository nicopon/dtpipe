using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Services;
using DtPipe.Tests.Helpers;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Arrow.Project;
using DtPipe.Transformers.Services;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Integration;

public class ArrowOwnershipTests
{
    private static RecordBatch MakeBatch(MemoryAllocator pool, int rowCount, int start = 0)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("val").DataType(DoubleType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(start + i);
            valBuilder.Append((start + i) * 1.5);
        }

        return new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(pool), valBuilder.Build(pool) }, rowCount);
    }

    [Fact]
    public async Task ApplyColumnarSegmentAsync_DisposesEveryInput_ThroughAnAliasingTransformer()
    {
        var pool = new TrackingMemoryPool();
        var executor = new PipelineExecutor(
            System.Linq.Enumerable.Empty<IRowToColumnarBridgeFactory>(),
            System.Linq.Enumerable.Empty<IColumnarToRowBridgeFactory>(),
            NullLogger<PipelineExecutor>.Instance);

        // Project with a rename returns a NEW batch that aliases every input column — the exact
        // case the ownership contract targets (transformer retains, segment runner disposes input).
        var project = new ProjectDataTransformer(new ProjectOptions { Rename = new[] { "id:ident" } });
        var columns = new List<PipeColumnInfo>
        {
            new("id", typeof(int), true),
            new("val", typeof(double), true),
        };
        await project.InitializeAsync(columns);

        async IAsyncEnumerable<RecordBatch> Source()
        {
            for (int b = 0; b < 4; b++)
            {
                yield return MakeBatch(pool, 32, b * 32);
                await Task.Yield();
            }
        }

        pool.ActiveAllocations.Should().Be(0);

        long lastId = -1;
        await foreach (var outBatch in executor.ApplyColumnarSegmentAsync(
            Source(), new List<IDataTransformer> { project }, Mock.Of<IExportProgress>(), default))
        {
            outBatch.Schema.FieldsList[0].Name.Should().Be("ident");
            var ids = (Int32Array)outBatch.Column(0);
            lastId = ids.GetValue(ids.Length - 1)!.Value;
            outBatch.Dispose(); // stand in for the writer, the downstream owner
        }

        lastId.Should().Be(127);
        pool.ActiveAllocations.Should().Be(0, "the segment runner disposes every input and the consumer disposes every output");
    }

    [Fact]
    public void FanOut_RetainAll_LetsEveryConsumerDisposeIndependently()
    {
        var pool = new TrackingMemoryPool();
        var source = MakeBatch(pool, 16);
        pool.ActiveAllocations.Should().BeGreaterThan(0);

        // What DagOrchestrator.BroadcastAsync does: one retained view per consumer, then drop
        // the broadcaster's own reference.
        var toColumnar = ArrowOwnership.RetainAll(source);
        var toRows = ArrowOwnership.RetainAll(source);
        source.Dispose();

        pool.ActiveAllocations.Should().BeGreaterThan(0, "two consumers still hold retained views");

        // Consumer A — columnar: reads then disposes.
        ((Int32Array)toColumnar.Column(0)).GetValue(3).Should().Be(3);
        toColumnar.Dispose();

        // Consumer B — row bridge: materialises every row, then disposes (ArrowMemoryChannelStreamReader path).
        var rows = ArrowRowConverter.FlattenBatch(toRows, 1024).SelectMany(m => m.ToArray()).ToList();
        rows.Should().HaveCount(16);
        ((int)rows[3]![0]!).Should().Be(3);
        toRows.Dispose();

        pool.ActiveAllocations.Should().Be(0, "the last consumer to dispose releases the shared buffers");
    }

    [Fact]
    public async Task Pipeline_ShouldDisposeAllBatches_WhenSuccessful()
    {
        // Arrange
        var pool = new TrackingMemoryPool();
        var rowCount = 100;
        
        // 1. Create source batch using tracking pool
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("val").DataType(DoubleType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(i);
            valBuilder.Append(i * 1.5);
        }

        // Pass pool to Build
        var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(pool), valBuilder.Build(pool) }, rowCount);
        
        // Initial state: memory is rented
        pool.ActiveAllocations.Should().BeGreaterThan(0);

        // 2. Setup Pipeline Components
        var mockJs = new Mock<IJsEngineProvider>();
        mockJs.Setup(j => j.GetEngine()).Returns(new Jint.Engine());

        // Transformer 1: Filter (Columnar) -> Will return NEW batch (clone) and dispose INPUT
        var filter = new FilterDataTransformer(new FilterOptions 
        { 
            Filters = new[] { "row.id > 50" },
        }, mockJs.Object);

        // Transformer 2: Project (Columnar) -> Will return NEW batch and dispose INPUT
        var project = new ProjectDataTransformer(new ProjectOptions 
        { 
            Project = new[] { "id", "val" } 
        });

        // Transformer 3: Spy -> Verifies intermediate disposal
        var spy = new SpyTransformer();

        // 3. Execution
        var columns = schema.FieldsList.Select(f => new PipeColumnInfo(f.Name, typeof(int), f.IsNullable)).ToList();
        await filter.InitializeAsync(columns);
        await project.InitializeAsync(columns);

        // Run the chain
        var batch1 = await filter.TransformBatchAsync(batch); 
        batch.Dispose(); // Manual disposal required in direct unit test calls
        
        var batch2 = await project.TransformBatchAsync(batch1!); 
        batch1?.Dispose(); // Manual disposal required in direct unit test calls
        
        var batch3 = await spy.TransformBatchAsync(batch2!); 
        batch2?.Dispose(); // Manual disposal required in direct unit test calls

        // Final Dispose (Simulating Writer)
        batch3?.Dispose();

        // Assert
        pool.ActiveAllocations.Should().Be(0, "All Arrow buffers should be disposed after pipeline completion.");
    }

    private class SpyTransformer : BaseColumnarTransformer
    {
        public override bool CanProcessColumnar { get; protected set; } = true;
        public override object?[]? Transform(IReadOnlyList<object?> row) => row as object?[] ?? row.ToArray();
        protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
        {
            // Just pass through
            return new ValueTask<RecordBatch?>(batch);
        }
    }
}
