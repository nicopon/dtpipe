using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Serialization.Reflection;
using DtPipe.Core.Infrastructure.Arrow;
using System.Runtime.InteropServices;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// Symmetric test to <see cref="DuckDBArrowUuidExtensionTests"/>:
/// verifies whether DataFusion preserves the official <c>arrow.uuid</c> canonical extension
/// when returning a schema via <c>dtfb_get_schema</c> (the Arrow C Data Interface).
///
/// If DataFusion preserves arrow.uuid, the schema returned by <c>DataFusionProcessor.InspectSchema</c>
/// correctly carries the extension metadata, and downstream writers using
/// <c>ArrowTypeMapper.GetClrTypeFromField</c> will resolve UUID columns to <c>Guid</c>.
///
/// If DataFusion strips it, the same schema gap that exists for DuckDB also exists here.
/// </summary>
public partial class DataFusionArrowUuidExtensionTests
{
    private readonly ITestOutputHelper _output;
    public DataFusionArrowUuidExtensionTests(ITestOutputHelper output) => _output = output;

    // Minimal DataFusion bridge P/Invoke — mirrors DataFusionBridge (internal to DtPipe.Processors).
    private const string DataFusionLib = "dtpipe_datafusion";

    [LibraryImport(DataFusionLib, EntryPoint = "dtfb_runtime_new")]
    private static partial nint RuntimeNew();

    [LibraryImport(DataFusionLib, EntryPoint = "dtfb_runtime_destroy")]
    private static partial void RuntimeDestroy(nint rt);

    [LibraryImport(DataFusionLib, EntryPoint = "dtfb_context_new")]
    private static partial nint ContextNew(nint rt);

    [LibraryImport(DataFusionLib, EntryPoint = "dtfb_context_destroy")]
    private static partial void ContextDestroy(nint ctx);

    [LibraryImport(DataFusionLib, EntryPoint = "dtfb_register_stream", StringMarshalling = StringMarshalling.Utf8)]
    private static unsafe partial int RegisterStream(nint ctx, string name, CArrowArrayStream* stream);

    [LibraryImport(DataFusionLib, EntryPoint = "dtfb_get_schema", StringMarshalling = StringMarshalling.Utf8)]
    private static unsafe partial int GetSchema(nint ctx, string sql, CArrowSchema* outSchema);

    [Fact]
    public unsafe void DataFusion_UuidColumn_GetSchema_PreservesArrowUuidExtension()
    {
        nint runtime;
        try { runtime = RuntimeNew(); }
        catch (DllNotFoundException ex)
        {
            Assert.Skip($"DataFusion native library not available: {ex.Message}");
            return;
        }

        if (runtime == nint.Zero)
            Assert.Skip("DataFusion runtime returned null — library may be incomplete.");

        var ctx = ContextNew(runtime);
        try
        {
            Assert.NotEqual(nint.Zero, ctx);

            // Build a RecordBatch with one UUID column using DtPipe's canonical Arrow UUID type.
            // This replicates what DataFusionProcessor receives via the Arrow channel.
            var uuidField = ArrowTypeMapper.GetField("id", typeof(Guid));
            var schema = new Schema(new[] { uuidField }, metadata: null);

            var uuidBuilder = new FixedSizeBinaryArrayBuilder(16);
            uuidBuilder.Append(ArrowTypeMapper.ToArrowUuidBytes(Guid.Parse("550e8400-e29b-41d4-a716-446655440000")));
            var uuidArray = uuidBuilder.Build();

            var batch = new RecordBatch(schema, new IArrowArray[] { uuidArray }, 1);

            // Register as a streaming source — same path as DataFusionProcessor.RegisterStreamingChannelSourceAsync
            var stream = new SingleBatchStream(schema, batch);
            var ffiStreamPtr = (CArrowArrayStream*)Marshal.AllocHGlobal(Marshal.SizeOf<CArrowArrayStream>());
            try
            {
                CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);
                Assert.Equal(0, RegisterStream(ctx, "t", ffiStreamPtr));
            }
            finally
            {
                Marshal.FreeHGlobal((nint)ffiStreamPtr);
            }

            // Ask DataFusion for the output schema of a passthrough SELECT.
            // Mirrors DataFusionProcessor.InspectSchema exactly.
            var ffiSchema = new CArrowSchema();
            var schemaResult = GetSchema(ctx, "SELECT id FROM t", &ffiSchema);
            Assert.Equal(0, schemaResult);
            var resultSchema = CArrowSchemaImporter.ImportSchema(&ffiSchema);

            var idField = Assert.Single(resultSchema.FieldsList);

            _output.WriteLine($"Field name    : {idField.Name}");
            _output.WriteLine($"Arrow type    : {idField.DataType.GetType().Name} ({idField.DataType})");
            _output.WriteLine($"Field metadata: {FormatMeta(idField.Metadata)}");

            string? extensionName = null;
            idField.Metadata?.TryGetValue("ARROW:extension:name", out extensionName);

            // Symmetric assertion to DuckDBArrowUuidExtensionTests.
            // PASS → DataFusion round-trips arrow.uuid; InspectSchema returns correct metadata.
            // FAIL → DataFusion strips extension metadata; same gap as DuckDB.
            Assert.True(
                string.Equals(extensionName, "arrow.uuid", StringComparison.OrdinalIgnoreCase),
                $"DataFusion did NOT preserve 'arrow.uuid' extension in GetSchema output. " +
                $"Actual Arrow type: {idField.DataType.GetType().Name}, " +
                $"metadata: {FormatMeta(idField.Metadata)}"
            );
        }
        finally
        {
            ContextDestroy(ctx);
            RuntimeDestroy(runtime);
        }
    }

    private static string FormatMeta(IReadOnlyDictionary<string, string>? meta) =>
        meta is null || meta.Count == 0
            ? "(none)"
            : string.Join(", ", meta.Select(kv => $"{kv.Key}={kv.Value}"));

    /// <summary>Minimal IArrowArrayStream that serves a single batch then signals end-of-stream.</summary>
    private sealed class SingleBatchStream : IArrowArrayStream
    {
        private readonly RecordBatch _batch;
        private bool _consumed;

        public Schema Schema { get; }

        public SingleBatchStream(Schema schema, RecordBatch batch)
        {
            Schema = schema;
            _batch = batch;
        }

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken ct = default)
        {
            if (_consumed) return new ValueTask<RecordBatch?>((RecordBatch?)null);
            _consumed = true;
            return new ValueTask<RecordBatch?>(_batch);
        }

        public void Dispose() { }
    }
}
