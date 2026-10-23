using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using DtPipe.Sessions;

namespace DtPipe.Benchmarks;

/// <summary>
/// What materialising a checkpoint costs, and how much of that is the encryption.
///
/// This is the number the "encrypt always, no opt-out" decision rests on. The cycle plan
/// estimated 50-100 ms per 100 MB from AES-GCM's hardware acceleration; an estimate is not a
/// measurement, and if the gap between <c>IpcOnly</c> and <c>IpcEncrypted</c> turned out to be
/// a large fraction of the write, the decision would have to be reopened — with the figure,
/// not with an impression.
///
/// Shape follows the macro bench's profile: 100k rows x 5 columns, with the two types that
/// dominate its cost (Guid, decimal).
/// </summary>
[MemoryDiagnoser]
public class CheckpointBenchmarks
{
    private const int Rows = 100_000;
    private const int BatchSize = 8_192;

    private byte[] _key = null!;
    private byte[] _header = null!;
    private List<RecordBatch> _batches = null!;
    private List<byte[]> _serialisedBatches = null!;
    private byte[] _encryptedFile = null!;
    private Schema _schema = null!;

    [GlobalSetup]
    public void Setup()
    {
        _key = RandomNumberGenerator.GetBytes(32);
        _header = CheckpointCipher.CreateHeader();
        _schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(new FixedSizeBinaryType(16)).Nullable(false))
            .Field(f => f.Name("amount").DataType(new Decimal128Type(18, 4)).Nullable(false))
            .Field(f => f.Name("label").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("qty").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("ts").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")).Nullable(false))
            .Build();

        // Kept alive across iterations: a real write serialises them every time, and the
        // baseline has to pay that too or the comparison flatters the cipher.
        _batches = BuildBatches().ToList();
        _serialisedBatches = _batches.Select(Serialise).ToList();

        using var file = new MemoryStream();
        long counter = 0;
        foreach (var b in _serialisedBatches) CheckpointCipher.WriteFrame(file, _key, _header, counter++, b);
        _encryptedFile = file.ToArray();
    }

    /// <summary>
    /// Materialising without the cipher: Arrow IPC serialisation and the write. This is the
    /// floor any checkpoint format pays, and the only baseline against which the encryption
    /// decision can honestly be judged — comparing the cipher to a memcpy of pre-serialised
    /// bytes would flatter neither side usefully.
    /// </summary>
    [Benchmark(Baseline = true)]
    public long MaterialiseUnencrypted()
    {
        using var file = new MemoryStream();
        foreach (var batch in _batches)
        {
            var bytes = Serialise(batch);
            file.Write(bytes);
        }
        return file.Length;
    }

    /// <summary>The same work with AES-GCM framing. The gap is what the guarantee costs.</summary>
    [Benchmark]
    public long MaterialiseEncrypted()
    {
        using var file = new MemoryStream();
        long counter = 0;
        foreach (var batch in _batches)
        {
            var bytes = Serialise(batch);
            CheckpointCipher.WriteFrame(file, _key, _header, counter++, bytes);
        }
        return file.Length;
    }

    /// <summary>The cipher in isolation, for a throughput figure independent of Arrow.</summary>
    [Benchmark]
    public long CipherOnly()
    {
        using var file = new MemoryStream();
        long counter = 0;
        foreach (var b in _serialisedBatches) CheckpointCipher.WriteFrame(file, _key, _header, counter++, b);
        return file.Length;
    }

    private static byte[] Serialise(RecordBatch batch)
    {
        using var ms = new MemoryStream();
        using (var w = new ArrowStreamWriter(ms, batch.Schema, leaveOpen: true))
        {
            w.WriteRecordBatch(batch);
            w.WriteEnd();
        }
        return ms.ToArray();
    }

    /// <summary>Total plaintext bytes, so the numbers above can be read as a throughput.</summary>
    [GlobalCleanup]
    public void ReportVolume()
        => Console.WriteLine($"// checkpoint payload: {_serialisedBatches.Sum(b => (long)b.Length):N0} bytes over {_serialisedBatches.Count} frames");

    /// <summary>Reading it back: decrypt and authenticate every frame.</summary>
    [Benchmark]
    public long ReadEncrypted()
    {
        using var file = new MemoryStream(_encryptedFile, writable: false);
        var header = new byte[CheckpointCipher.HeaderSize];
        file.ReadExactly(header);

        long bytes = 0;
        while (CheckpointCipher.ReadFrame(file, _key, header) is { } frame) bytes += frame.Length;
        return bytes;
    }

    private IEnumerable<RecordBatch> BuildBatches()
    {
        var rnd = new Random(1);
        for (var offset = 0; offset < Rows; offset += BatchSize)
        {
            var n = Math.Min(BatchSize, Rows - offset);
            var ids = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
            var amounts = new Decimal128Array.Builder(new Decimal128Type(18, 4));
            var labels = new StringArray.Builder();
            var qty = new Int32Array.Builder();
            var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));

            for (var i = 0; i < n; i++)
            {
                ids.Append(Guid.NewGuid().ToByteArray());
                amounts.Append((decimal)Math.Round(rnd.NextDouble() * 10_000, 4));
                labels.Append($"label-{offset + i}");
                qty.Append(rnd.Next(1, 1000));
                ts.Append(DateTimeOffset.UnixEpoch.AddSeconds(offset + i));
            }

            yield return new RecordBatch(_schema,
                [ids.Build(), amounts.Build(), labels.Build(), qty.Build(), ts.Build()], n);
        }
    }
}
