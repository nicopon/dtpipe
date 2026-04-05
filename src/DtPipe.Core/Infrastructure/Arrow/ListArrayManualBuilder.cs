using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Core.Infrastructure.Arrow;

internal class ListArrayManualBuilder : IArrowArrayBuilder
{
    private readonly ListType _type;
    private readonly IArrowArrayBuilder _valueBuilder;
    private readonly List<int> _offsets = new();
    private readonly List<bool> _validity = new();
    private int _nullCount;

    public ListArrayManualBuilder(ListType type)
    {
        _type = type;
        _valueBuilder = ArrowTypeMapper.CreateBuilder(type.ValueDataType);
        _offsets.Add(0);
    }

    public void AppendValue(object? value)
    {
        if (value == null || value is not System.Collections.IEnumerable enumerable)
        {
            AppendNull();
        }
        else
        {
            int count = 0;
            foreach (var item in enumerable)
            {
                ArrowTypeMapper.AppendValue(_valueBuilder, item);
                count++;
            }
            _offsets.Add(_offsets.Last() + count);
            _validity.Add(true);
        }
    }

    public void AppendNull()
    {
        _validity.Add(false);
        _nullCount++;
        _offsets.Add(_offsets.Last());
    }

    public IArrowArray Build()
    {
        int length = _validity.Count;
        var values = ArrowTypeMapper.BuildArray(_valueBuilder);
        var offsetBuf = new ArrowBuffer.Builder<int>(_offsets.Count).AppendRange(_offsets).Build();
        var validBuf = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
        var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);
        return new ListArray(_type, length, offsetBuf, values, validBuilder.Build(), _nullCount);
    }

    public void Clear() { _validity.Clear(); _offsets.Clear(); _offsets.Add(0); _nullCount = 0; /* Clear not in interface */ }
    public int Length => _validity.Count;
}
