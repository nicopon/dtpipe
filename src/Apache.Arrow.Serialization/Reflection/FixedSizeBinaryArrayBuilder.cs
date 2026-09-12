using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization.Reflection;

/// <summary>
/// Builds a <see cref="FixedSizeBinaryArray"/> of arbitrary byte width.
///
/// <see cref="Apache.Arrow"/> C# 22.x has no public <c>FixedSizeBinaryArray.Builder</c>,
/// so this class builds the underlying validity bitmap and data buffer manually.
///
/// Cells are written straight into one growable buffer, never into a per-cell array:
/// holding a <c>List&lt;byte[]&gt;</c> instead costs ×57 on a Guid column, which is the
/// reader's hottest — the P5 / P8 pair in
/// <c>tests/DtPipe.Benchmarks/ParquetReaderCellBenchmarks.cs</c> measures exactly that gap.
/// </summary>
public sealed class FixedSizeBinaryArrayBuilder : IArrowArrayBuilder
{
    public int ByteWidth { get; }

    private byte[] _data = [];
    private byte[] _validity = [];
    private int _capacity;
    private int _length;
    private int _nullCount;

    public FixedSizeBinaryArrayBuilder(int byteWidth)
    {
        if (byteWidth <= 0)
            throw new ArgumentOutOfRangeException(nameof(byteWidth), "ByteWidth must be strictly positive.");

        ByteWidth = byteWidth;
    }

    /// <param name="capacity">Number of cells, not bytes.</param>
    public void Reserve(int capacity) => EnsureCapacity(capacity);

    /// <summary>
    /// Appends one cell. Input longer than <see cref="ByteWidth"/> is truncated, shorter is
    /// zero-padded on the right.
    /// </summary>
    public void Append(ReadOnlySpan<byte> bytes)
    {
        EnsureCapacity(_length + 1);

        var slot = _data.AsSpan(_length * ByteWidth, ByteWidth);
        int copied = Math.Min(bytes.Length, ByteWidth);
        bytes.Slice(0, copied).CopyTo(slot);
        if (copied < ByteWidth) slot.Slice(copied).Clear();

        _validity[_length >> 3] |= (byte)(1 << (_length & 7));
        _length++;
    }

    public void AppendNull()
    {
        EnsureCapacity(_length + 1);

        _data.AsSpan(_length * ByteWidth, ByteWidth).Clear();
        _validity[_length >> 3] &= (byte)~(1 << (_length & 7));

        _nullCount++;
        _length++;
    }

    public int Length => _length;

    public void Clear()
    {
        // Both buffers are reused, so the bits of the cells just dropped have to go: Append
        // only ever sets a validity bit, and AppendNull writes over its own data slot.
        System.Array.Clear(_validity);
        _length = 0;
        _nullCount = 0;
    }

    public IArrowArray Build()
    {
        int n = _length;
        int validBytes = (n + 7) / 8;

        var dataBuilder = new ArrowBuffer.Builder<byte>(n * ByteWidth);
        dataBuilder.Append(_data.AsSpan(0, n * ByteWidth));

        var validBuilder = new ArrowBuffer.Builder<byte>(validBytes);
        validBuilder.Append(_validity.AsSpan(0, validBytes));

        var data = new ArrayData(
            new FixedSizeBinaryType(ByteWidth),
            length: n,
            nullCount: _nullCount,
            offset: 0,
            buffers: new[] { validBuilder.Build(), dataBuilder.Build() });

        return new FixedSizeBinaryArray(data);
    }

    private void EnsureCapacity(int cells)
    {
        if (cells <= _capacity) return;

        int grown = Math.Max(cells, _capacity == 0 ? 64 : _capacity * 2);
        System.Array.Resize(ref _data, grown * ByteWidth);
        System.Array.Resize(ref _validity, (grown + 7) / 8);
        _capacity = grown;
    }
}
