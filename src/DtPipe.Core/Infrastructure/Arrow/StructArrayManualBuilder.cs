using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Core.Infrastructure.Arrow;

internal class StructArrayManualBuilder : IArrowArrayBuilder
{
    private readonly StructType _type;
    private readonly List<IArrowArrayBuilder> _builders;
    private readonly List<bool> _validity = new();
    private int _nullCount;

    public StructArrayManualBuilder(StructType type)
    {
        _type = type;
        _builders = new List<IArrowArrayBuilder>(type.Fields.Count);
        foreach (var field in type.Fields)
        {
            _builders.Add(ArrowTypeMapper.CreateBuilder(field.DataType));
        }
    }

    public void AppendValue(object? value)
    {
        if (value == null) { AppendNull(); }
        else
        {
            _validity.Add(true);
            if (value is System.Collections.IDictionary dict)
            {
                for (int i = 0; i < _type.Fields.Count; i++)
                {
                    var field = _type.Fields[i];
                    var childValue = dict.Contains(field.Name) ? dict[field.Name] : null;
                    ArrowTypeMapper.AppendValue(_builders[i], childValue);
                }
            }
            else
            {
                var props = value.GetType().GetProperties();
                for (int i = 0; i < _type.Fields.Count; i++)
                {
                    var field = _type.Fields[i];
                    var prop = props.FirstOrDefault(p => string.Equals(p.Name, field.Name, StringComparison.OrdinalIgnoreCase));
                    var childValue = prop?.GetValue(value);
                    ArrowTypeMapper.AppendValue(_builders[i], childValue);
                }
            }
        }
    }

    public void AppendNull()
    {
        _validity.Add(false);
        _nullCount++;
        for (int i = 0; i < _builders.Count; i++) ArrowTypeMapper.AppendNull(_builders[i]);
    }

    public IArrowArray Build()
    {
        int length = _validity.Count;
        var children = _builders.Select(b => ArrowTypeMapper.BuildArray(b)).ToList();
        var validBuf = new byte[(length + 7) / 8];
        for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
        var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);
        return new StructArray(_type, length, children, validBuilder.Build(), _nullCount);
    }

    public void Clear() { _validity.Clear(); _nullCount = 0; foreach(var b in _builders) { /* Clear not in interface, skip */ } }
    public int Length => _validity.Count;
}
