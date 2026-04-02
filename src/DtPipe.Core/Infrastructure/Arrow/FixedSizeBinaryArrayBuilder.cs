using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Builds a <see cref="FixedSizeBinaryArray"/> of arbitrary byte width.
///
/// <see cref="Apache.Arrow"/> C# 22.x has no public <c>FixedSizeBinaryArray.Builder</c>,
/// so this class builds the underlying validity bitmap and data buffer manually.
/// </summary>
public sealed class FixedSizeBinaryArrayBuilder : IArrowArrayBuilder
{
    public int ByteWidth { get; }

    private readonly List<byte[]> _values = new();
    private readonly List<bool> _valid = new();
    private int _nullCount;

    public FixedSizeBinaryArrayBuilder(int byteWidth)
    {
        if (byteWidth <= 0)
            throw new ArgumentOutOfRangeException(nameof(byteWidth), "ByteWidth must be strictly positive.");
        
        ByteWidth = byteWidth;
    }

    /// <summary>Appends a fixed-size byte array value.</summary>
    public void Append(ReadOnlySpan<byte> bytes)
    {
        var arr = new byte[ByteWidth];
        bytes.Slice(0, Math.Min(bytes.Length, ByteWidth)).CopyTo(arr);
        _values.Add(arr);
        _valid.Add(true);
    }

    /// <summary>Appends a null entry.</summary>
    public void AppendNull()
    {
        _values.Add(new byte[ByteWidth]);
        _valid.Add(false);
        _nullCount++;
    }

    /// <summary>Number of values appended so far.</summary>
    public int Length => _values.Count;

    /// <summary>Clears all appended values.</summary>
    public void Clear()
    {
        _values.Clear();
        _valid.Clear();
        _nullCount = 0;
    }

    /// <summary>Builds the <see cref="FixedSizeBinaryArray"/> from accumulated values.</summary>
    public IArrowArray Build()
    {
        int n = _values.Count;

        // Use ArrowBuffer.Builder<byte> so each buffer has a proper IMemoryOwner.
        // ArrowBuffer(byte[]) wraps raw memory without an owner, which crashes the
        // Apache Arrow C Data Interface exporter (CArrowArrayExporter.ReleaseArray).

        // Data buffer: n * ByteWidth packed bytes
        var dataBuilder = new ArrowBuffer.Builder<byte>(ByteWidth * n);
        for (int i = 0; i < n; i++)
            dataBuilder.AppendRange(_values[i]);

        // Validity bitmap (LSB-first per Arrow spec)
        var validBuf = new byte[(n + 7) / 8];
        for (int i = 0; i < n; i++)
            if (_valid[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
        
        var validBuilder = new ArrowBuffer.Builder<byte>((n + 7) / 8);
        validBuilder.AppendRange(validBuf);

        var data = new ArrayData(
            new FixedSizeBinaryType(ByteWidth),
            length: n,
            nullCount: _nullCount,
            offset: 0,
            buffers: new[] { validBuilder.Build(), dataBuilder.Build() });

        return new FixedSizeBinaryArray(data);
    }
}
