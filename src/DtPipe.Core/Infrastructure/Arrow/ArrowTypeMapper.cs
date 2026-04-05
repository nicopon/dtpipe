using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Serialization.Mapping;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Centralized mapper for CLR to Arrow types and vice-versa.
/// Provides unified logic for schema generation, builder creation, and value extraction.
///
/// Extensions (like arrow.uuid) are processed strictly via explicit metadata.
/// There are no heuristics or default Guid-inference logic.
///
/// IMPORTANT – inheritance order in switches:
///   Decimal128Type/Decimal256Type inherit from FixedSizeBinaryType.
///   Decimal128Array/Decimal256Array inherit from FixedSizeBinaryArray.
///   Always match Decimal variants BEFORE FixedSizeBinary in switch arms.
/// </summary>
public static class ArrowTypeMapper
{
    // ── UUID byte-order helpers ──────────────────────────────────────────────

    /// <summary>
    /// Converts a .NET Guid (little-endian first 3 components) to RFC 4122 big-endian bytes
    /// suitable for canonical Arrow UUID storage.
    /// </summary>
    public static byte[] ToArrowUuidBytes(Guid guid) => ArrowTypeMap.ToArrowUuidBytes(guid);

    /// <summary>
    /// Converts RFC 4122 big-endian UUID bytes (from an Arrow binary column) back to a .NET Guid.
    /// </summary>
    public static Guid FromArrowUuidBytes(ReadOnlySpan<byte> b) => ArrowTypeMap.FromArrowUuidBytes(b);

    // ── Type mappings ────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the CLR type for a given Arrow type.
    /// Performs only unambiguous, direct mappings — no heuristics.
    /// <see cref="FixedSizeBinaryType"/> always maps to <c>typeof(byte[])</c> regardless of byte width;
    /// use <see cref="GetClrTypeFromField"/> to resolve semantic types via extension metadata (e.g. arrow.uuid → Guid).
    /// </summary>
    public static Type GetClrType(IArrowType type) => Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetClrType(type);

    /// <summary>
    /// Returns the CLR type for a given Arrow <see cref="Field"/>, checking extension metadata first.
    /// Unlike <see cref="GetClrType(IArrowType)"/>, this method resolves semantic extension types:
    /// a field with storage type <see cref="FixedSizeBinaryType"/>(16) and metadata
    /// <c>ARROW:extension:name = arrow.uuid</c> returns <c>typeof(Guid)</c>.
    /// Falls through to <see cref="GetClrType(IArrowType)"/> for all other types.
    /// </summary>
    public static Type GetClrTypeFromField(Field field) => Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetClrTypeFromField(field);

    /// <summary>
    /// Extracts the value at <paramref name="index"/> from <paramref name="array"/>,
    /// resolving extension types from <paramref name="field"/> metadata when available.
    /// For a field with <c>arrow.uuid</c> extension and a <see cref="FixedSizeBinaryArray"/>,
    /// returns a <see cref="Guid"/> (converted via <see cref="FromArrowUuidBytes"/>).
    /// Falls through to <see cref="GetValue(IArrowArray,int)"/> for all other cases.
    /// </summary>
    public static object? GetValueForField(IArrowArray array, Field field, int index)
        => ArrowTypeMap.GetValue(array, index, field);

    public static Apache.Arrow.Serialization.Mapping.ArrowTypeResult GetLogicalType(Type clrType) => Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetLogicalType(clrType);

    public static Apache.Arrow.Field GetField(string name, Type clrType, bool isNullable = true) => 
        Apache.Arrow.Serialization.Mapping.ArrowTypeMap.GetField(name, GetLogicalType(clrType), isNullable);


    public static IArrowArrayBuilder CreateBuilder(IArrowType type)
    {
        return type switch
        {
            BooleanType => new BooleanArray.Builder(),
            Int8Type => new Int8Array.Builder(),
            Int16Type => new Int16Array.Builder(),
            Int32Type => new Int32Array.Builder(),
            Int64Type => new Int64Array.Builder(),
            UInt8Type => new UInt8Array.Builder(),
            UInt16Type => new UInt16Array.Builder(),
            UInt32Type => new UInt32Array.Builder(),
            UInt64Type => new UInt64Array.Builder(),
            FloatType => new FloatArray.Builder(),
            DoubleType => new DoubleArray.Builder(),
            StringType => new StringArray.Builder(),
            BinaryType => new BinaryArray.Builder(),
            // Decimal types BEFORE FixedSizeBinaryType (inheritance order)
            Decimal128Type t => new Decimal128Array.Builder(t),
            Decimal256Type t => new Decimal256Array.Builder(t),
            // FixedSizeBinary maps to the generic builder
            FixedSizeBinaryType fst => new FixedSizeBinaryArrayBuilder(fst.ByteWidth),
            Date32Type => new Date32Array.Builder(),
            Date64Type => new Date64Array.Builder(),
            TimestampType t => new TimestampArray.Builder(t),
            DurationType t => new DurationArray.Builder(t),
            Time32Type t => new Time32Array.Builder(t),
            Time64Type t => new Time64Array.Builder(t),
            StructType st => new StructArrayManualBuilder(st),
            ListType lt => new ListArrayManualBuilder(lt),
            _ => throw new NotSupportedException($"Unsupported Arrow type for builder: {type.Name}")
        };
    }

    public static object? GetValue(IArrowArray array, int index)
        => ArrowTypeMap.GetValue(array, index);

    public static void AppendNull(IArrowArrayBuilder builder)
    {
        switch (builder)
        {
            case BooleanArray.Builder b: b.AppendNull(); break;
            case Int8Array.Builder b: b.AppendNull(); break;
            case Int16Array.Builder b: b.AppendNull(); break;
            case Int32Array.Builder b: b.AppendNull(); break;
            case Int64Array.Builder b: b.AppendNull(); break;
            case UInt8Array.Builder b: b.AppendNull(); break;
            case UInt16Array.Builder b: b.AppendNull(); break;
            case UInt32Array.Builder b: b.AppendNull(); break;
            case UInt64Array.Builder b: b.AppendNull(); break;
            case FloatArray.Builder b: b.AppendNull(); break;
            case DoubleArray.Builder b: b.AppendNull(); break;
            case StringArray.Builder b: b.AppendNull(); break;
            case BinaryArray.Builder b: b.AppendNull(); break;
            case FixedSizeBinaryArrayBuilder b: b.AppendNull(); break;
            case Decimal128Array.Builder b: b.AppendNull(); break;
            case Decimal256Array.Builder b: b.AppendNull(); break;
            case Date32Array.Builder b: b.AppendNull(); break;
            case Date64Array.Builder b: b.AppendNull(); break;
            case TimestampArray.Builder b: b.AppendNull(); break;
            case DurationArray.Builder b: b.AppendNull(); break;
            case Time32Array.Builder b: b.AppendNull(); break;
            case Time64Array.Builder b: b.AppendNull(); break;
            case StructArrayManualBuilder b: b.AppendNull(); break;
            case ListArrayManualBuilder b: b.AppendNull(); break;
        }
    }

    public static IArrowArray BuildArray(IArrowArrayBuilder builder)
    {
        return builder switch
        {
            BooleanArray.Builder b => b.Build(),
            Int8Array.Builder b => b.Build(),
            Int16Array.Builder b => b.Build(),
            Int32Array.Builder b => b.Build(),
            Int64Array.Builder b => b.Build(),
            UInt8Array.Builder b => b.Build(),
            UInt16Array.Builder b => b.Build(),
            UInt32Array.Builder b => b.Build(),
            UInt64Array.Builder b => b.Build(),
            FloatArray.Builder b => b.Build(),
            DoubleArray.Builder b => b.Build(),
            StringArray.Builder b => b.Build(),
            BinaryArray.Builder b => b.Build(),
            FixedSizeBinaryArrayBuilder b => b.Build(),
            Decimal128Array.Builder b => b.Build(),
            Decimal256Array.Builder b => b.Build(),
            Date32Array.Builder b => b.Build(),
            Date64Array.Builder b => b.Build(),
            TimestampArray.Builder b => b.Build(),
            DurationArray.Builder b => b.Build(),
            Time32Array.Builder b => b.Build(),
            Time64Array.Builder b => b.Build(),
            StructArrayManualBuilder b => b.Build(),
            ListArrayManualBuilder b => b.Build(),
            _ => throw new NotSupportedException($"Unsupported builder type for BuildArray: {builder.GetType().Name}")
        };
    }

    public static void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            AppendNull(builder);
            return;
        }

        switch (builder)
        {
            case StringArray.Builder b: b.Append(value.ToString()); break;
            case Int8Array.Builder b: b.Append(Convert.ToSByte(value)); break;
            case Int16Array.Builder b: b.Append(Convert.ToInt16(value)); break;
            case Int32Array.Builder b: b.Append(Convert.ToInt32(value)); break;
            case Int64Array.Builder b: b.Append(Convert.ToInt64(value)); break;
            case UInt8Array.Builder b: b.Append(Convert.ToByte(value)); break;
            case UInt16Array.Builder b: b.Append(Convert.ToUInt16(value)); break;
            case UInt32Array.Builder b: b.Append(Convert.ToUInt32(value)); break;
            case UInt64Array.Builder b: b.Append(Convert.ToUInt64(value)); break;
            case DoubleArray.Builder b: b.Append(Convert.ToDouble(value)); break;
            case FloatArray.Builder b: b.Append(Convert.ToSingle(value)); break;
            case BooleanArray.Builder b: b.Append(Convert.ToBoolean(value)); break;
            case Decimal128Array.Builder b: b.Append(Convert.ToDecimal(value)); break;
            case Decimal256Array.Builder b: b.Append(Convert.ToDecimal(value)); break;
            case FixedSizeBinaryArrayBuilder b:
                if (value is Guid guid) b.Append(ToArrowUuidBytes(guid));
                else if (value is byte[] fixedBytes && fixedBytes.Length == b.ByteWidth) b.Append(fixedBytes);
                else b.AppendNull();
                break;
            case BinaryArray.Builder b:
                // Generic binary builder: byte[] passed through as-is
                if (value is byte[] binBytes) b.Append((System.Collections.Generic.IEnumerable<byte>)binBytes);
                else b.AppendNull();
                break;
            case Date32Array.Builder b: b.Append(Convert.ToDateTime(value)); break;
            case Date64Array.Builder b: b.Append(Convert.ToDateTime(value)); break;
            case TimestampArray.Builder b:
                if (value is DateTimeOffset dto) b.Append(dto);
                else if (value is DateTime dt) b.Append(dt);
                else b.Append(Convert.ToDateTime(value));
                break;
            case StructArrayManualBuilder b:
                b.AppendValue(value);
                break;
            case ListArrayManualBuilder b:
                b.AppendValue(value);
                break;
            default:
                AppendNull(builder);
                break;
        }
    }


    public static void AppendArrayValue(IArrowArrayBuilder builder, IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            AppendNull(builder);
            return;
        }

        switch (array)
        {
            case BooleanArray a: ((BooleanArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int8Array a: ((Int8Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int16Array a: ((Int16Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int32Array a: ((Int32Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Int64Array a: ((Int64Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt8Array a: ((UInt8Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt16Array a: ((UInt16Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt32Array a: ((UInt32Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case UInt64Array a: ((UInt64Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case FloatArray a: ((FloatArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case DoubleArray a: ((DoubleArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case StringArray a: ((StringArray.Builder)builder).Append(a.GetString(index)); break;
            case BinaryArray a: ((BinaryArray.Builder)builder).Append(a.GetBytes(index)); break;
            // Decimal arrays BEFORE FixedSizeBinaryArray (inheritance order)
            case Decimal128Array a: ((Decimal128Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Decimal256Array a: ((Decimal256Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            // FixedSizeBinaryArray: route to FixedSizeBinaryArrayBuilder if available, else BinaryArray.Builder
            case FixedSizeBinaryArray a when builder is FixedSizeBinaryArrayBuilder fb:
                fb.Append(a.GetBytes(index)); break;
            case FixedSizeBinaryArray a:
                ((BinaryArray.Builder)builder).Append(a.GetBytes(index)); break;
            case Date32Array a: ((Date32Array.Builder)builder).Append(a.GetDateTime(index)!.Value); break;
            case Date64Array a: ((Date64Array.Builder)builder).Append(a.GetDateTime(index)!.Value); break;
            case TimestampArray a: ((TimestampArray.Builder)builder).Append(a.GetTimestamp(index)!.Value); break;
            case DurationArray a: ((DurationArray.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Time32Array a: ((Time32Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            case Time64Array a: ((Time64Array.Builder)builder).Append(a.GetValue(index)!.Value); break;
            default:
                // Boxed fallback for any missed types
                AppendValue(builder, GetValue(array, index));
                break;
        }
    }

    // ── Manual Builders for Nested Types ─────────────────────────────────────

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
                _builders.Add(CreateBuilder(field.DataType));
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
            var children = _builders.Select(b => BuildArray(b)).ToList();
            var validBuf = new byte[(length + 7) / 8];
            for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
            var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);
            return new StructArray(_type, length, children, validBuilder.Build(), _nullCount);
        }

        public void Clear() { _validity.Clear(); _nullCount = 0; foreach(var b in _builders) { /* Clear not in interface, skip */ } }
        public int Length => _validity.Count;
    }

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
            _valueBuilder = CreateBuilder(type.ValueDataType);
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
            var values = BuildArray(_valueBuilder);
            var offsetBuf = new ArrowBuffer.Builder<int>(_offsets.Count).AppendRange(_offsets).Build();
            var validBuf = new byte[(length + 7) / 8];
            for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
            var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);
            return new ListArray(_type, length, offsetBuf, values, validBuilder.Build(), _nullCount);
        }

        public void Clear() { _validity.Clear(); _offsets.Clear(); _offsets.Add(0); _nullCount = 0; /* Clear not in interface */ }
        public int Length => _validity.Count;
    }
}
