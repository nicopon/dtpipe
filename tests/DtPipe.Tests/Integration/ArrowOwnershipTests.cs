using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using DtPipe.Transformers.Columnar.Filter;
using DtPipe.Transformers.Columnar.Project;
using DtPipe.Transformers.Services;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Integration;

public class ArrowOwnershipTests
{
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
        var filter = new FilterDataTransformer(new FilterTransformerOptions 
        { 
            Filters = new[] { "id > 50" },
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
        var batch1 = await filter.TransformBatchAsync(batch); // batch is disposed here
        var batch2 = await project.TransformBatchAsync(batch1!); // batch1 is disposed here
        var batch3 = await spy.TransformBatchAsync(batch2!); // batch2 is disposed here

        // Final Dispose (Simulating Writer)
        batch3?.Dispose();

        // Assert
        pool.ActiveAllocations.Should().Be(0, "All Arrow buffers should be disposed after pipeline completion.");
    }

    private class SpyTransformer : BaseColumnarTransformer
    {
        public override bool CanProcessColumnar { get; protected set; } = true;
        public override object?[]? Transform(object?[] row) => row;
        protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
        {
            // Just pass through
            return new ValueTask<RecordBatch?>(batch);
        }
    }
}
