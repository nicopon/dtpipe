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
/// What both row views refuse. A transformer hands a script the same view from row to row, and a write
/// to a column is cleared when the view moves, but extensibility taken away from the view would stay on
/// it for the next row. Each view throws a <c>TypeError</c> naming the operation rather than answering
/// false, so <c>Reflect.preventExtensions</c> fails as loudly as <c>Object.preventExtensions</c>,
/// <c>Object.seal</c> and <c>Object.freeze</c>. A prototype set on the view is not refused (Jint lets no
/// type outside it override <c>SetPrototypeOf</c>): the view takes <c>Object.prototype</c> back each time
/// it moves, so the change lasts for its row only, as a write to a column does.
/// </summary>
internal static class RowViewRefusals
{
    public static JavaScriptException PreventExtensions(Engine engine)
        => new(engine.Intrinsics.TypeError, "Cannot prevent extensions of a row (Object.preventExtensions, seal or freeze)");
}

/// <summary>
/// What both row views share: the column names a script may read, the writes it makes to the view (kept
/// beside the row and cleared when the view moves), the keys and property lookups the engine asks of an
/// object, and the refusals of <see cref="RowViewRefusals"/>. A view differs only in where a column's value
/// comes from (<see cref="ReadColumn"/>) and in how it moves to the next row, which ends in
/// <see cref="ResetWrites"/>.
/// </summary>
internal abstract class RowViewObject : ObjectInstance, IRowObjectView
{
    private readonly string[] _columnNames;
    private readonly Dictionary<string, int> _nameToIndex;
    private readonly List<JsValue> _cachedKeys;
    private readonly ErrorConstructor _refErrorCtor;
    private readonly ObjectInstance _objectPrototype;
    private Dictionary<string, JsValue>? _overrides;

    protected RowViewObject(Engine engine, IReadOnlyList<string> columnNames, int exposedColumnCount, ErrorConstructor refErrorCtor)
        : base(engine)
    {
        _objectPrototype = engine.Intrinsics.Object.PrototypeObject;
        Prototype = _objectPrototype;
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

    /// <summary>The value of column <paramref name="idx"/> in the row the view stands on.</summary>
    protected abstract JsValue ReadColumn(int idx);

    /// <summary>
    /// Ends the writes of the row the view leaves: they are cleared and the prototype is
    /// <c>Object.prototype</c> again (<see cref="RowViewRefusals"/>).
    /// </summary>
    protected void ResetWrites()
    {
        _overrides?.Clear();
        Prototype = _objectPrototype;
    }

    public override bool PreventExtensions() => throw RowViewRefusals.PreventExtensions(Engine);

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
}

/// <summary>
/// Reusable lazy JS object view over a CLR row. Avoids creating a JsObject, copying unused
/// columns, and evaluating a JavaScript Proxy per row while preserving ReferenceError checks.
/// </summary>
internal sealed class JsRowObjectInstance : RowViewObject
{
    private IReadOnlyList<object?> _row = System.Array.Empty<object?>();

    public JsRowObjectInstance(Engine engine, IReadOnlyList<string> columnNames, int exposedColumnCount, ErrorConstructor refErrorCtor)
        : base(engine, columnNames, exposedColumnCount, refErrorCtor)
    {
    }

    /// <summary>
    /// Moves the view to <paramref name="row"/>: the previous row's writes are cleared and its prototype
    /// is <c>Object.prototype</c> again (<see cref="RowViewRefusals"/>).
    /// </summary>
    public void SetRow(IReadOnlyList<object?> row)
    {
        _row = row;
        ResetWrites();
    }

    protected override JsValue ReadColumn(int idx) => JsValueConversion.ClrToJsValue(Engine, _row[idx]);
}

/// <summary>
/// Reusable lazy JS object view backed directly by an Arrow RecordBatch at a mutable row cursor.
/// Column readers are compiled lazily on first property read per column per batch.
/// </summary>
internal sealed class ArrowRowObjectInstance : RowViewObject
{
    private readonly Func<int, JsValue>?[] _readers;
    private RecordBatch? _boundBatch;
    private int _rowIndex;

    public ArrowRowObjectInstance(Engine engine, IReadOnlyList<string> columnNames, int exposedColumnCount, ErrorConstructor refErrorCtor)
        : base(engine, columnNames, exposedColumnCount, refErrorCtor)
    {
        _readers = new Func<int, JsValue>?[exposedColumnCount];
    }

    public void BindBatch(RecordBatch batch)
    {
        if (ReferenceEquals(_boundBatch, batch))
            return;

        _boundBatch = batch;
        System.Array.Clear(_readers);
    }

    /// <summary><see cref="JsRowObjectInstance.SetRow"/> at <paramref name="index"/> of the bound batch.</summary>
    public void SetRowIndex(int index)
    {
        _rowIndex = index;
        ResetWrites();
    }

    protected override JsValue ReadColumn(int idx)
    {
        var reader = _readers[idx] ??= CompileJsReader(
            Engine,
            _boundBatch!.Column(idx),
            _boundBatch.Schema.GetFieldByIndex(idx));
        return reader(_rowIndex);
    }

    internal static Func<int, JsValue> CompileJsReader(Engine engine, IArrowArray array, Field field)
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
