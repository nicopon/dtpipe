using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Jint;
using Jint.Native;
using Jint.Native.Error;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using JsTypes = Jint.Runtime.Types;

namespace DtPipe.Transformers.Services;

internal interface IRowObjectView
{
    bool HasOverrides { get; }
}

/// <summary>
/// Reusable lazy JS object view over a CLR row. Avoids creating a JsObject, copying unused
/// columns, and evaluating a JavaScript Proxy per row while preserving ReferenceError checks.
/// </summary>
internal sealed class JsRowObjectInstance : ObjectInstance, IRowObjectView
{
    private readonly string[] _columnNames;
    private readonly Dictionary<string, int> _nameToIndex;
    private readonly List<JsValue> _cachedKeys;
    private readonly ErrorConstructor _refErrorCtor;
    private IReadOnlyList<object?> _row = System.Array.Empty<object?>();
    private Dictionary<string, JsValue>? _overrides;

    public JsRowObjectInstance(Engine engine, IReadOnlyList<string> columnNames, int exposedColumnCount, ErrorConstructor refErrorCtor)
        : base(engine)
    {
        Prototype = engine.Intrinsics.Object.PrototypeObject;
        _refErrorCtor = refErrorCtor;
        _columnNames = new string[exposedColumnCount];
        _nameToIndex = new Dictionary<string, int>(exposedColumnCount, StringComparer.Ordinal);
        _cachedKeys = new List<JsValue>(exposedColumnCount);

        for (int i = 0; i < exposedColumnCount; i++)
        {
            var name = columnNames[i];
            _columnNames[i] = name;
            _nameToIndex[name] = i;
            _cachedKeys.Add(JsString.Create(name));
        }
    }

    public bool HasOverrides => _overrides is { Count: > 0 };

    public void SetRow(IReadOnlyList<object?> row)
    {
        _row = row;
        _overrides?.Clear();
    }

    public override JsValue Get(JsValue property, JsValue receiver)
    {
        if (property.IsString())
        {
            var name = property.AsString();
            if (_overrides != null && _overrides.TryGetValue(name, out var overVal))
                return overVal;

            if (_nameToIndex.TryGetValue(name, out int idx))
                return JsValueConversion.ClrToJsValue(Engine, _row[idx]);

            if (base.HasProperty(property))
                return base.Get(property, receiver);

            throw new JavaScriptException(_refErrorCtor, $"Column '{name}' not found in schema");
        }

        return base.Get(property, receiver);
    }

    public override bool Set(JsValue property, JsValue value, JsValue receiver)
    {
        if (property.IsString())
        {
            (_overrides ??= new Dictionary<string, JsValue>(StringComparer.Ordinal))[property.AsString()] = value;
            return true;
        }

        return base.Set(property, value, receiver);
    }

    public override PropertyDescriptor GetOwnProperty(JsValue property)
    {
        if (property.IsString())
        {
            var name = property.AsString();
            if (_overrides != null && _overrides.TryGetValue(name, out var overVal))
                return new PropertyDescriptor(overVal, PropertyFlag.ConfigurableEnumerableWritable);

            if (_nameToIndex.TryGetValue(name, out int idx))
                return new PropertyDescriptor(JsValueConversion.ClrToJsValue(Engine, _row[idx]), PropertyFlag.ConfigurableEnumerableWritable);
        }

        return PropertyDescriptor.Undefined;
    }

    public override bool HasProperty(JsValue property)
    {
        if (property.IsString())
        {
            var name = property.AsString();
            if ((_overrides != null && _overrides.ContainsKey(name)) || _nameToIndex.ContainsKey(name))
                return true;
        }

        return base.HasProperty(property);
    }

    public override List<JsValue> GetOwnPropertyKeys(JsTypes types = JsTypes.String | JsTypes.Symbol)
    {
        if ((types & JsTypes.String) == JsTypes.Empty)
            return [];

        if (_overrides == null || _overrides.Count == 0)
            return _cachedKeys;

        var keys = new List<JsValue>(_cachedKeys.Count + _overrides.Count);
        keys.AddRange(_cachedKeys);
        foreach (var key in _overrides.Keys)
        {
            if (!_nameToIndex.ContainsKey(key))
                keys.Add(JsString.Create(key));
        }
        return keys;
    }

    public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
    {
        for (int i = 0; i < _columnNames.Length; i++)
        {
            var name = _columnNames[i];
            var val = _overrides != null && _overrides.TryGetValue(name, out var overVal)
                ? overVal
                : JsValueConversion.ClrToJsValue(Engine, _row[i]);
            yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                _cachedKeys[i],
                new PropertyDescriptor(val, PropertyFlag.ConfigurableEnumerableWritable));
        }

        if (_overrides != null)
        {
            foreach (var kvp in _overrides)
            {
                if (!_nameToIndex.ContainsKey(kvp.Key))
                {
                    yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                        JsString.Create(kvp.Key),
                        new PropertyDescriptor(kvp.Value, PropertyFlag.ConfigurableEnumerableWritable));
                }
            }
        }
    }
}

/// <summary>
/// Reusable lazy JS object view backed directly by an Arrow RecordBatch at a mutable row cursor.
/// Column readers are compiled lazily on first property read per column per batch.
/// </summary>
internal sealed class ArrowRowObjectInstance : ObjectInstance, IRowObjectView
{
    private readonly string[] _columnNames;
    private readonly Dictionary<string, int> _nameToIndex;
    private readonly List<JsValue> _cachedKeys;
    private readonly ErrorConstructor _refErrorCtor;
    private readonly Func<int, JsValue>?[] _readers;
    private RecordBatch? _boundBatch;
    private int _rowIndex;
    private Dictionary<string, JsValue>? _overrides;

    public ArrowRowObjectInstance(Engine engine, IReadOnlyList<string> columnNames, int exposedColumnCount, ErrorConstructor refErrorCtor)
        : base(engine)
    {
        Prototype = engine.Intrinsics.Object.PrototypeObject;
        _refErrorCtor = refErrorCtor;
        _columnNames = new string[exposedColumnCount];
        _nameToIndex = new Dictionary<string, int>(exposedColumnCount, StringComparer.Ordinal);
        _cachedKeys = new List<JsValue>(exposedColumnCount);
        _readers = new Func<int, JsValue>?[exposedColumnCount];

        for (int i = 0; i < exposedColumnCount; i++)
        {
            var name = columnNames[i];
            _columnNames[i] = name;
            _nameToIndex[name] = i;
            _cachedKeys.Add(JsString.Create(name));
        }
    }

    public bool HasOverrides => _overrides is { Count: > 0 };

    public void BindBatch(RecordBatch batch)
    {
        if (ReferenceEquals(_boundBatch, batch))
            return;

        _boundBatch = batch;
        System.Array.Clear(_readers);
    }

    public void SetRowIndex(int index)
    {
        _rowIndex = index;
        _overrides?.Clear();
    }

    private JsValue ReadColumn(int idx)
    {
        var reader = _readers[idx] ??= CompileJsReader(
            Engine,
            _boundBatch!.Column(idx),
            _boundBatch.Schema.GetFieldByIndex(idx));
        return reader(_rowIndex);
    }

    public override JsValue Get(JsValue property, JsValue receiver)
    {
        if (property.IsString())
        {
            var name = property.AsString();
            if (_overrides != null && _overrides.TryGetValue(name, out var overVal))
                return overVal;

            if (_nameToIndex.TryGetValue(name, out int idx))
                return ReadColumn(idx);

            if (base.HasProperty(property))
                return base.Get(property, receiver);

            throw new JavaScriptException(_refErrorCtor, $"Column '{name}' not found in schema");
        }

        return base.Get(property, receiver);
    }

    public override bool Set(JsValue property, JsValue value, JsValue receiver)
    {
        if (property.IsString())
        {
            (_overrides ??= new Dictionary<string, JsValue>(StringComparer.Ordinal))[property.AsString()] = value;
            return true;
        }

        return base.Set(property, value, receiver);
    }

    public override PropertyDescriptor GetOwnProperty(JsValue property)
    {
        if (property.IsString())
        {
            var name = property.AsString();
            if (_overrides != null && _overrides.TryGetValue(name, out var overVal))
                return new PropertyDescriptor(overVal, PropertyFlag.ConfigurableEnumerableWritable);

            if (_nameToIndex.TryGetValue(name, out int idx))
                return new PropertyDescriptor(ReadColumn(idx), PropertyFlag.ConfigurableEnumerableWritable);
        }

        return PropertyDescriptor.Undefined;
    }

    public override bool HasProperty(JsValue property)
    {
        if (property.IsString())
        {
            var name = property.AsString();
            if ((_overrides != null && _overrides.ContainsKey(name)) || _nameToIndex.ContainsKey(name))
                return true;
        }

        return base.HasProperty(property);
    }

    public override List<JsValue> GetOwnPropertyKeys(JsTypes types = JsTypes.String | JsTypes.Symbol)
    {
        if ((types & JsTypes.String) == JsTypes.Empty)
            return [];

        if (_overrides == null || _overrides.Count == 0)
            return _cachedKeys;

        var keys = new List<JsValue>(_cachedKeys.Count + _overrides.Count);
        keys.AddRange(_cachedKeys);
        foreach (var key in _overrides.Keys)
        {
            if (!_nameToIndex.ContainsKey(key))
                keys.Add(JsString.Create(key));
        }
        return keys;
    }

    public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
    {
        for (int i = 0; i < _columnNames.Length; i++)
        {
            var name = _columnNames[i];
            var val = _overrides != null && _overrides.TryGetValue(name, out var overVal)
                ? overVal
                : ReadColumn(i);
            yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                _cachedKeys[i],
                new PropertyDescriptor(val, PropertyFlag.ConfigurableEnumerableWritable));
        }

        if (_overrides != null)
        {
            foreach (var kvp in _overrides)
            {
                if (!_nameToIndex.ContainsKey(kvp.Key))
                {
                    yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                        JsString.Create(kvp.Key),
                        new PropertyDescriptor(kvp.Value, PropertyFlag.ConfigurableEnumerableWritable));
                }
            }
        }
    }

    private static Func<int, JsValue> CompileJsReader(Engine engine, IArrowArray array, Field field)
    {
        // Metadata-bearing columns (arrow.uuid, arrow.bool8, arrow.opaque, undeclared decimals)
        // must flow through ArrowTypeMap so logical decoding matches row mode.
        if (field.HasMetadata)
        {
            var metaReader = ArrowTypeMap.CompileValueReader(array, field);
            return i => JsValueConversion.ClrToJsValue(engine, metaReader(i));
        }

        switch (array)
        {
            case DoubleArray d:
                return d.NullCount == 0
                    ? i => JsNumber.Create(d.Values[i])
                    : i => d.IsNull(i) ? JsValue.Null : JsNumber.Create(d.Values[i]);
            case FloatArray f:
                return f.NullCount == 0
                    ? i => JsNumber.Create(f.Values[i])
                    : i => f.IsNull(i) ? JsValue.Null : JsNumber.Create(f.Values[i]);
            case Int32Array i32:
                return i32.NullCount == 0
                    ? i => JsNumber.Create(i32.Values[i])
                    : i => i32.IsNull(i) ? JsValue.Null : JsNumber.Create(i32.Values[i]);
            case Int64Array i64:
                return i64.NullCount == 0
                    ? i => JsNumber.Create(i64.Values[i])
                    : i => i64.IsNull(i) ? JsValue.Null : JsNumber.Create(i64.Values[i]);
            case Int16Array i16:
                return i16.NullCount == 0
                    ? i => JsNumber.Create(i16.Values[i])
                    : i => i16.IsNull(i) ? JsValue.Null : JsNumber.Create(i16.Values[i]);
            case Int8Array i8:
                return i8.NullCount == 0
                    ? i => JsNumber.Create(i8.Values[i])
                    : i => i8.IsNull(i) ? JsValue.Null : JsNumber.Create(i8.Values[i]);
            case UInt8Array u8:
                return u8.NullCount == 0
                    ? i => JsNumber.Create(u8.Values[i])
                    : i => u8.IsNull(i) ? JsValue.Null : JsNumber.Create(u8.Values[i]);
            case UInt16Array u16:
                return u16.NullCount == 0
                    ? i => JsNumber.Create(u16.Values[i])
                    : i => u16.IsNull(i) ? JsValue.Null : JsNumber.Create(u16.Values[i]);
            case UInt32Array u32:
                return u32.NullCount == 0
                    ? i => JsNumber.Create((double)u32.Values[i])
                    : i => u32.IsNull(i) ? JsValue.Null : JsNumber.Create((double)u32.Values[i]);
            case UInt64Array u64:
                return u64.NullCount == 0
                    ? i => JsNumber.Create((double)u64.Values[i])
                    : i => u64.IsNull(i) ? JsValue.Null : JsNumber.Create((double)u64.Values[i]);
            case Decimal128Array dec:
                return dec.NullCount == 0
                    ? i => JsNumber.Create(dec.GetValue(i)!.Value)
                    : i => dec.IsNull(i) ? JsValue.Null : JsNumber.Create(dec.GetValue(i)!.Value);
            case StringArray s:
                return s.NullCount == 0
                    ? i => JsString.Create(s.GetString(i))
                    : i => s.IsNull(i) ? JsValue.Null : JsString.Create(s.GetString(i));
            case BooleanArray b:
                return b.NullCount == 0
                    ? i => b.GetValue(i)!.Value ? JsBoolean.True : JsBoolean.False
                    : i => b.IsNull(i) ? JsValue.Null : (b.GetValue(i)!.Value ? JsBoolean.True : JsBoolean.False);
            default:
                var fallback = ArrowTypeMap.CompileValueReader(array, field);
                return i => JsValueConversion.ClrToJsValue(engine, fallback(i));
        }
    }
}
