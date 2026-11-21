using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Builds an Arrow <see cref="StringArray"/> directly from UTF-8 spans without managed string
/// allocations or UTF-16 transcoding.
/// </summary>
internal struct Utf8ArrowColumnBuilder
{
    private readonly int _capacity;
    private ArrowBuffer.Builder<int> _offsets;
    private ArrowBuffer.Builder<byte> _data;
    private ArrowBuffer.BitmapBuilder? _nullBitmap;
    private int _currentOffset;
    private int _length;
    private int _nullCount;

    public Utf8ArrowColumnBuilder(int rowCount, int estimatedBytesPerRow)
    {
        _capacity = rowCount;
        _offsets = new ArrowBuffer.Builder<int>(rowCount + 1);
        _offsets.Append(0);
        _data = new ArrowBuffer.Builder<byte>(Math.Max(64, rowCount * Math.Max(4, estimatedBytesPerRow)));
        _nullBitmap = null;
        _currentOffset = 0;
        _length = 0;
        _nullCount = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<byte> utf8)
    {
        _data.Append(utf8);
        _currentOffset += utf8.Length;
        _offsets.Append(_currentOffset);
        _nullBitmap?.Append(true);
        _length++;
    }

    public void AppendNull()
    {
        if (_nullBitmap is null)
        {
            _nullBitmap = new ArrowBuffer.BitmapBuilder(_capacity);
            for (int i = 0; i < _length; i++)
                _nullBitmap.Append(true);
        }

        _nullBitmap.Append(false);
        _nullCount++;
        _offsets.Append(_currentOffset);
        _length++;
    }

    public StringArray Build()
    {
        var nullBuf = _nullBitmap?.Build() ?? ArrowBuffer.Empty;
        return new StringArray(_length, _offsets.Build(), _data.Build(), nullBuf, _nullCount);
    }
}
