using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization.Reflection;

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

    public void Reserve(int capacity)
    {
        if (capacity > _values.Capacity) _values.Capacity = capacity;
        if (capacity > _valid.Capacity) _valid.Capacity = capacity;
    }

    public void Append(ReadOnlySpan<byte> bytes)
    {
        var arr = new byte[ByteWidth];
        bytes.Slice(0, Math.Min(bytes.Length, ByteWidth)).CopyTo(arr);
        _values.Add(arr);
        _valid.Add(true);
    }

    public void AppendNull()
    {
        _values.Add(new byte[ByteWidth]);
        _valid.Add(false);
        _nullCount++;
    }

    public int Length => _values.Count;

    public void Clear()
    {
        _values.Clear();
        _valid.Clear();
        _nullCount = 0;
    }

    public IArrowArray Build()
    {
        int n = _values.Count;
        var dataBuilder = new ArrowBuffer.Builder<byte>(ByteWidth * n);
        for (int i = 0; i < n; i++)
            dataBuilder.AppendRange(_values[i]);

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
