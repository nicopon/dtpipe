using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Adapters.Arrow;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Arrow;

[Collection(SessionStateCollection.Name)]
public class ArrowCompressionTests : IDisposable
{
    private readonly string _tempDir;

    public ArrowCompressionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"arrow_comp_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(CompressionCodecType.Lz4Frame)]
    [InlineData(CompressionCodecType.Zstd)]
    public async Task Raw_ArrowStreamWriter_And_Reader_RoundTrip_WithCompression(CompressionCodecType? codec)
    {
        var schema = new Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var idArray = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
        var nameArray = new StringArray.Builder().Append("Alice").Append("Bob").Append("Charlie").Build();
        var batch = new RecordBatch(schema, [idArray, nameArray], 3);

        using var ms = new MemoryStream();
        var factory = new Apache.Arrow.Compression.CompressionCodecFactory();
        var options = codec.HasValue
            ? new IpcOptions { CompressionCodec = codec.Value, CompressionCodecFactory = factory }
            : null;
        var writer = options != null
            ? new ArrowStreamWriter(ms, schema, leaveOpen: true, options)
            : new ArrowStreamWriter(ms, schema, leaveOpen: true);
        await writer.WriteRecordBatchAsync(batch);
        await writer.WriteEndAsync();

        ms.Position = 0;
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(ms, factory);
        var readBatch = await reader.ReadNextRecordBatchAsync();

        readBatch.Should().NotBeNull();
        readBatch!.Length.Should().Be(3);
        readBatch.ColumnCount.Should().Be(2);

        var readId = (Int32Array)readBatch.Column(0);
        readId.GetValue(0).Should().Be(1);
        readId.GetValue(1).Should().Be(2);
        readId.GetValue(2).Should().Be(3);

        var readName = (StringArray)readBatch.Column(1);
        readName.GetString(0).Should().Be("Alice");
        readName.GetString(1).Should().Be("Bob");
        readName.GetString(2).Should().Be("Charlie");
    }

    [Theory]
    [InlineData("lz4")]
    [InlineData("lz4_frame")]
    [InlineData("zstd")]
    [InlineData("uncompressed")]
    [InlineData(null)]
    public async Task ArrowAdapter_WriterAndReader_RoundTrip_WithExplicitOption(string? compression)
    {
        var streamPath = Path.Combine(_tempDir, $"stream_{Guid.NewGuid():N}.ipc");
        var columns = new PipeColumnInfo[]
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        var writerOptions = new ArrowWriterOptions { Compression = compression };
        var writer = new ArrowAdapterDataWriter(streamPath, writerOptions);
        await writer.InitializeAsync(columns);

        var schema = new Schema.Builder()
            .Field(new Field("id", Int32Type.Default, false))
            .Field(new Field("name", StringType.Default, true))
            .Build();

        var idArray = new Int32Array.Builder().Append(10).Append(20).Build();
        var nameArray = new StringArray.Builder().Append("Foo").Append("Bar").Build();
        var batch = new RecordBatch(schema, [idArray, nameArray], 2);

        await writer.WriteRecordBatchAsync(batch);
        await writer.CompleteAsync();
        await writer.DisposeAsync();

        var reader = new ArrowAdapterStreamReader(streamPath, new ArrowReaderOptions());
        await reader.OpenAsync();

        var batches = new List<RecordBatch>();
        await foreach (var rb in reader.ReadRecordBatchesAsync())
        {
            batches.Add(rb);
        }
        await reader.DisposeAsync();

        batches.Should().HaveCount(1);
        var readBatch = batches[0];
        readBatch.Length.Should().Be(2);

        var readId = (Int32Array)readBatch.Column(0);
        readId.GetValue(0).Should().Be(10);
        readId.GetValue(1).Should().Be(20);

        var readName = (StringArray)readBatch.Column(1);
        readName.GetString(0).Should().Be("Foo");
        readName.GetString(1).Should().Be("Bar");
    }

    [Fact]
    public async Task ArrowAdapter_Writer_UsesEnvironmentVariable_WhenWritingSequentialStream()
    {
        var prevEnv = Environment.GetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION");
        try
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", "lz4");
            var streamPath = Path.Combine(_tempDir, $"stream_env_{Guid.NewGuid():N}.ipc");
            var columns = new PipeColumnInfo[] { new("val", typeof(string), true) };

            var writer = new ArrowAdapterDataWriter(streamPath, new ArrowWriterOptions());
            await writer.InitializeAsync(columns);

            var schema = new Schema.Builder()
                .Field(new Field("val", StringType.Default, true))
                .Build();

            // Large repetitive text to make compression noticeable
            var builder = new StringArray.Builder();
            for (int i = 0; i < 500; i++) builder.Append("A long repetitive string that compresses very well!");
            var batch = new RecordBatch(schema, [builder.Build()], 500);

            await writer.WriteRecordBatchAsync(batch);
            await writer.CompleteAsync();
            await writer.DisposeAsync();

            var compressedSize = new FileInfo(streamPath).Length;

            // Now write uncompressed for comparison
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", null);
            var uncompressedPath = Path.Combine(_tempDir, $"stream_uncomp_{Guid.NewGuid():N}.ipc");
            var uncompWriter = new ArrowAdapterDataWriter(uncompressedPath, new ArrowWriterOptions());
            await uncompWriter.InitializeAsync(columns);

            builder = new StringArray.Builder();
            for (int i = 0; i < 500; i++) builder.Append("A long repetitive string that compresses very well!");
            var uncompBatch = new RecordBatch(schema, [builder.Build()], 500);

            await uncompWriter.WriteRecordBatchAsync(uncompBatch);
            await uncompWriter.CompleteAsync();
            await uncompWriter.DisposeAsync();

            var uncompressedSize = new FileInfo(uncompressedPath).Length;

            // Compressed size should be noticeably smaller
            compressedSize.Should().BeLessThan(uncompressedSize / 2);

            // Read back compressed stream
            var reader = new ArrowAdapterStreamReader(streamPath, new ArrowReaderOptions());
            await reader.OpenAsync();
            var readBatches = new List<RecordBatch>();
            await foreach (var rb in reader.ReadRecordBatchesAsync())
            {
                readBatches.Add(rb);
            }
            await reader.DisposeAsync();

            readBatches.Should().HaveCount(1);
            readBatches[0].Length.Should().Be(500);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", prevEnv);
        }
    }

    [Fact]
    public async Task ArrowAdapter_FileFormat_IgnoresEnvironmentVariable_UnlessExplicit()
    {
        var prevEnv = Environment.GetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION");
        try
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", "lz4");
            var filePath = Path.Combine(_tempDir, $"file_{Guid.NewGuid():N}.arrow");
            var columns = new PipeColumnInfo[] { new("val", typeof(string), true) };

            // When writing a .arrow file without explicit Compression, it remains uncompressed
            var writer = new ArrowAdapterDataWriter(filePath, new ArrowWriterOptions());
            await writer.InitializeAsync(columns);

            var schema = new Schema.Builder()
                .Field(new Field("val", StringType.Default, true))
                .Build();

            var builder = new StringArray.Builder();
            for (int i = 0; i < 500; i++) builder.Append("A long repetitive string that compresses very well!");
            var batch = new RecordBatch(schema, [builder.Build()], 500);

            await writer.WriteRecordBatchAsync(batch);
            await writer.CompleteAsync();
            await writer.DisposeAsync();

            var defaultFileSize = new FileInfo(filePath).Length;

            // Explicitly compressed .arrow file
            var explicitCompressedPath = Path.Combine(_tempDir, $"file_explicit_{Guid.NewGuid():N}.arrow");
            var explicitWriter = new ArrowAdapterDataWriter(explicitCompressedPath, new ArrowWriterOptions { Compression = "lz4" });
            await explicitWriter.InitializeAsync(columns);

            builder = new StringArray.Builder();
            for (int i = 0; i < 500; i++) builder.Append("A long repetitive string that compresses very well!");
            var explicitBatch = new RecordBatch(schema, [builder.Build()], 500);

            await explicitWriter.WriteRecordBatchAsync(explicitBatch);
            await explicitWriter.CompleteAsync();
            await explicitWriter.DisposeAsync();

            var explicitCompressedSize = new FileInfo(explicitCompressedPath).Length;

            // Default file format should be uncompressed, so explicit compressed should be smaller
            explicitCompressedSize.Should().BeLessThan(defaultFileSize / 2);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", prevEnv);
        }
    }

    [Fact]
    public async Task ArrowAdapter_Writer_UsesEnvironmentVariable_CompressionLevel_WhenWritingSequentialStream()
    {
        var prevCodec = Environment.GetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION");
        var prevLevel = Environment.GetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION_LEVEL");
        try
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", "zstd");
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION_LEVEL", "7");
            var streamPath = Path.Combine(_tempDir, $"stream_level_{Guid.NewGuid():N}.ipc");
            var columns = new PipeColumnInfo[] { new("val", typeof(string), true) };

            var writer = new ArrowAdapterDataWriter(streamPath, new ArrowWriterOptions());
            await writer.InitializeAsync(columns);

            var schema = new Schema.Builder()
                .Field(new Field("val", StringType.Default, true))
                .Build();

            var builder = new StringArray.Builder();
            for (int i = 0; i < 200; i++) builder.Append("Repetitive payload for compression level verification");
            var batch = new RecordBatch(schema, [builder.Build()], 200);

            await writer.WriteRecordBatchAsync(batch);
            await writer.CompleteAsync();
            await writer.DisposeAsync();

            var reader = new ArrowAdapterStreamReader(streamPath, new ArrowReaderOptions());
            await reader.OpenAsync();
            var readBatches = new List<RecordBatch>();
            await foreach (var rb in reader.ReadRecordBatchesAsync())
            {
                readBatches.Add(rb);
            }
            await reader.DisposeAsync();

            readBatches.Should().HaveCount(1);
            readBatches[0].Length.Should().Be(200);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", prevCodec);
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION_LEVEL", prevLevel);
        }
    }

    [Fact]
    public async Task ArrowAdapter_Writer_Throws_WhenInvalidCompressionLevelInEnv()
    {
        var prevCodec = Environment.GetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION");
        var prevLevel = Environment.GetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION_LEVEL");
        try
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", "lz4");
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION_LEVEL", "not-a-number");
            var streamPath = Path.Combine(_tempDir, $"stream_invalid_level_{Guid.NewGuid():N}.ipc");
            var columns = new PipeColumnInfo[] { new("val", typeof(string), true) };

            var writer = new ArrowAdapterDataWriter(streamPath, new ArrowWriterOptions());
            await writer.InitializeAsync(columns);

            var schema = new Schema.Builder()
                .Field(new Field("val", StringType.Default, true))
                .Build();

            var batch = new RecordBatch(schema, [new StringArray.Builder().Append("test").Build()], 1);

            var act = async () => await writer.WriteRecordBatchAsync(batch);
            await act.Should().ThrowAsync<ArgumentException>()
                .WithMessage("*DTPIPE_ARROW_COMPRESSION_LEVEL*");
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION", prevCodec);
            Environment.SetEnvironmentVariable("DTPIPE_ARROW_COMPRESSION_LEVEL", prevLevel);
        }
    }

    [Fact]
    public async Task ArrowAdapter_WriterAndReader_RoundTrip_WithExplicitCompressionLevel()
    {
        var streamPath = Path.Combine(_tempDir, $"stream_explicit_level_{Guid.NewGuid():N}.ipc");
        var columns = new PipeColumnInfo[] { new("val", typeof(string), true) };

        var writerOptions = new ArrowWriterOptions { Compression = "zstd", CompressionLevel = 5 };
        var writer = new ArrowAdapterDataWriter(streamPath, writerOptions);
        await writer.InitializeAsync(columns);

        var schema = new Schema.Builder()
            .Field(new Field("val", StringType.Default, true))
            .Build();

        var builder = new StringArray.Builder();
        for (int i = 0; i < 100; i++) builder.Append($"String-entry-{i}");
        var batch = new RecordBatch(schema, [builder.Build()], 100);

        await writer.WriteRecordBatchAsync(batch);
        await writer.CompleteAsync();
        await writer.DisposeAsync();

        var reader = new ArrowAdapterStreamReader(streamPath, new ArrowReaderOptions());
        await reader.OpenAsync();
        var readBatches = new List<RecordBatch>();
        await foreach (var rb in reader.ReadRecordBatchesAsync())
        {
            readBatches.Add(rb);
        }
        await reader.DisposeAsync();

        readBatches.Should().HaveCount(1);
        readBatches[0].Length.Should().Be(100);
    }
}
