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
    [InlineData("zstd")]
    [InlineData("none")]
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

    private static RecordBatch RepetitiveBatch(int rows)
    {
        var schema = new Schema.Builder().Field(new Field("val", StringType.Default, true)).Build();
        var builder = new StringArray.Builder();
        for (int i = 0; i < rows; i++) builder.Append("A long repetitive string that compresses very well!");
        return new RecordBatch(schema, [builder.Build()], rows);
    }

    private async Task<long> WriteSizeAsync(string path, ArrowWriterOptions options)
    {
        var writer = new ArrowAdapterDataWriter(path, options);
        await writer.InitializeAsync([new PipeColumnInfo("val", typeof(string), true)]);
        await writer.WriteRecordBatchAsync(RepetitiveBatch(500));
        await writer.CompleteAsync();
        await writer.DisposeAsync();
        return new FileInfo(path).Length;
    }

    [Theory]
    [InlineData("x.ipc")]
    [InlineData("x.arrow")]
    public async Task ArrowAdapter_Writer_IsUncompressed_ByDefault_WhateverTheTarget(string name)
    {
        var plain = await WriteSizeAsync(Path.Combine(_tempDir, "plain_" + name), new ArrowWriterOptions());
        var none = await WriteSizeAsync(Path.Combine(_tempDir, "none_" + name), new ArrowWriterOptions { Compression = "none" });
        var lz4 = await WriteSizeAsync(Path.Combine(_tempDir, "lz4_" + name), new ArrowWriterOptions { Compression = "lz4" });

        none.Should().Be(plain);
        lz4.Should().BeLessThan(plain / 2);
    }

    [Theory]
    [InlineData("gzip")]
    [InlineData("uncompressed")]
    [InlineData("lz4_frame")]
    public async Task ArrowAdapter_Writer_RefusesAnUnknownCodec_BeforeTheTargetOpens(string codec)
    {
        var path = Path.Combine(_tempDir, "typo.ipc");

        // Built by the factory, so a dry run (which never initializes the writer) sees it too.
        var build = () => new ArrowAdapterDataWriter(path, new ArrowWriterOptions { Compression = codec });
        build.Should().Throw<ArgumentException>().WithMessage("*none, lz4, zstd*");

        // Changed after construction, the check at InitializeAsync still runs before the file exists.
        var options = new ArrowWriterOptions();
        var writer = new ArrowAdapterDataWriter(path, options);
        options.Compression = codec;
        var act = async () => await writer.InitializeAsync([new PipeColumnInfo("val", typeof(string), true)]);
        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*none, lz4, zstd*");
        File.Exists(path).Should().BeFalse();
    }

    [Fact]
    public async Task ArrowAdapter_Writer_RoundTrips_WithZstdAtALevel()
    {
        var path = Path.Combine(_tempDir, "level.ipc");
        await WriteSizeAsync(path, new ArrowWriterOptions { Compression = "zstd", CompressionLevel = 7 });

        var reader = new ArrowAdapterStreamReader(path, new ArrowReaderOptions());
        await reader.OpenAsync();
        var rows = 0;
        await foreach (var rb in reader.ReadRecordBatchesAsync()) rows += rb.Length;
        await reader.DisposeAsync();
        rows.Should().Be(500);
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
