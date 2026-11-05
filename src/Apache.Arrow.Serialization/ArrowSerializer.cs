using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Serialization.Reflection;
using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Apache.Arrow.Serialization.Dynamic;
using Apache.Arrow.Serialization.Accessors;

namespace Apache.Arrow.Serialization;

public static class ArrowSerializer
{
    private static readonly ConcurrentDictionary<Type, object> _cache = new();

    public static async Task<RecordBatch> SerializeAsync<T>(IEnumerable<T> data, MemoryAllocator? allocator = null)
    {
        if (typeof(T) == typeof(object))
        {
            var s = new TypedSerializer<T>();
            return await Task.Run(() => s.Serialize(data, allocator));
        }

        var serializer = (TypedSerializer<T>)_cache.GetOrAdd(typeof(T), _ => new TypedSerializer<T>());
        return await Task.Run(() => serializer.Serialize(data, allocator));
    }

    public static async Task<RecordBatch> SerializeAsync<T>(IEnumerable<T> data, Schema schema, MemoryAllocator? allocator = null)
    {
        var s = new TypedSerializer<T>(schema);
        return await Task.Run(() => s.Serialize(data, allocator));
    }

    internal class CapacityInfo
    {
        public int Count { get; set; } = 0;
        public Dictionary<string, CapacityInfo> Children { get; } = new();
    }

    private class TypedSerializer<T>
    {
        private Schema? _schema;
        private TypeAccessor? _rootAccessor;
        private readonly bool _isDynamic;

        public TypedSerializer()
        {
            _isDynamic = typeof(T) == typeof(object) ||
                         typeof(T).Name == "JsonObject" ||
                         typeof(IDictionary).IsAssignableFrom(typeof(T));
            if (!_isDynamic)
            {
                _schema = ArrowReflectionEngine.GetSchema(typeof(T));
                _rootAccessor = TypeAccessor.Create(typeof(T));
            }
        }

        public TypedSerializer(Schema schema)
        {
            _isDynamic = typeof(T) == typeof(object) ||
                         typeof(T).Name == "JsonObject" ||
                         typeof(IDictionary).IsAssignableFrom(typeof(T));
            _schema = schema;
            _rootAccessor = _isDynamic
                ? (TypeAccessor)new DynamicStructTypeAccessor(schema)
                : TypeAccessor.Create(typeof(T));
        }

        public RecordBatch Serialize(IEnumerable<T> data, MemoryAllocator? allocator = null)
        {
            var list = data is ICollection<T> coll ? coll : data.ToList();
            int totalCount = list.Count;

            if (_isDynamic && _schema == null)
            {
                var first = list.FirstOrDefault(x => x != null);
                if (first == null) throw new InvalidOperationException("Cannot infer schema from empty/null dynamic collection.");

                IDictionary? dict = first as IDictionary;
                if (dict == null)
                {
                    if (first is System.Text.Json.Nodes.JsonObject jo) dict = new JsonObjectWrapper(jo);
                    else if (first is IDictionary<string, object?> gd) dict = new DictionaryWrapper(gd);
                }

                if (dict == null) throw new NotSupportedException($"Dynamic type {first.GetType().Name} is not supported. Use ExpandoObject or JsonObject.");

                _schema = new Schema(ArrowReflectionEngine.GetSchema(dict).FieldsList, null);
                _rootAccessor = new DynamicStructTypeAccessor(_schema);
            }

            var rootAccessor = (StructTypeAccessorBase)_rootAccessor!;

            // Step 1: Sampling for Capacity Estimation
            int sampleSize = Math.Min(100, totalCount);
            var sample = list.Take(sampleSize);
            var sampleEstimates = new CapacityInfo { Count = sampleSize };
            foreach (var item in sample)
            {
                _rootAccessor!.CollectCapacity(item, sampleEstimates);
            }

            // Step 2: Extrapolate estimates to global count
            var globalEstimates = Extrapolate(sampleEstimates, sampleSize, totalCount);

            // Step 3: Create pre-dimensionioned builders
            var topLevelBuilders = rootAccessor.CreateChildBuilders(allocator, globalEstimates);

            long record = 0;
            foreach (var item in list)
            {
                record++;
                try
                {
                    rootAccessor.AppendChildren(item!, topLevelBuilders);
                }
                catch (ArrowValueException fault)
                {
                    throw fault.AtRecord(record);
                }
            }

            var arrays = new List<IArrowArray>();
            for (int i = 0; i < topLevelBuilders.Count; i++)
            {
                arrays.Add(rootAccessor.ChildAccessors[i].Build(topLevelBuilders[i]));
            }

            return new RecordBatch(_schema!, arrays, totalCount);
        }

        private CapacityInfo Extrapolate(CapacityInfo sample, int sampleSize, int totalCount)
        {
            var global = new CapacityInfo { Count = totalCount };
            if (sampleSize == 0) return global;

            double scale = (double)totalCount / sampleSize;

            foreach (var kvp in sample.Children)
            {
                var childGlobal = new CapacityInfo
                {
                    Count = (int)Math.Ceiling(kvp.Value.Count * scale)
                };
                global.Children[kvp.Key] = childGlobal;
                // Recursively extrapolate deeper nested counts
                foreach (var nested in kvp.Value.Children)
                    childGlobal.Children[nested.Key] = ExtrapolateNested(nested.Value, kvp.Value.Count, childGlobal.Count);
            }
            return global;
        }

        private CapacityInfo ExtrapolateNested(CapacityInfo sample, int sampleParentCount, int globalParentCount)
        {
            var global = new CapacityInfo
            {
                Count = globalParentCount > 0 && sampleParentCount > 0
                ? (int)Math.Ceiling(sample.Count * ((double)globalParentCount / sampleParentCount))
                : 0
            };
            foreach (var kvp in sample.Children)
                global.Children[kvp.Key] = ExtrapolateNested(kvp.Value, sample.Count, global.Count);
            return global;
        }
    }

    internal static void AppendNull(IArrowArrayBuilder builder)
    {
        if (builder == null) return;
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
            case Decimal128Array.Builder b: b.AppendNull(); break;
            case Decimal256Array.Builder b: b.AppendNull(); break;
            case FixedSizeBinaryArrayBuilder b: b.AppendNull(); break;
            case TimestampArray.Builder b: b.AppendNull(); break;
            case DurationArray.Builder b: b.AppendNull(); break;
            case Date32Array.Builder b: b.AppendNull(); break;
            case Date64Array.Builder b: b.AppendNull(); break;
            case Time32Array.Builder b: b.AppendNull(); break;
            case Time64Array.Builder b: b.AppendNull(); break;
            case ListArrayManualBuilder b: b.AppendNull(); break;
            case MapArrayManualBuilder b: b.AppendNull(); break;
            case StructArrayManualBuilder b: b.AppendNull(); break;
            default: builder.GetType().GetMethod("AppendNull")?.Invoke(builder, null); break;
        }
    }

    internal static void Reserve(IArrowArrayBuilder builder, int count)
    {
        if (builder == null || count <= 0) return;
        switch (builder)
        {
            case BooleanArray.Builder b: b.Reserve(count); break;
            case Int8Array.Builder b: b.Reserve(count); break;
            case Int16Array.Builder b: b.Reserve(count); break;
            case Int32Array.Builder b: b.Reserve(count); break;
            case Int64Array.Builder b: b.Reserve(count); break;
            case UInt8Array.Builder b: b.Reserve(count); break;
            case UInt16Array.Builder b: b.Reserve(count); break;
            case UInt32Array.Builder b: b.Reserve(count); break;
            case UInt64Array.Builder b: b.Reserve(count); break;
            case FloatArray.Builder b: b.Reserve(count); break;
            case DoubleArray.Builder b: b.Reserve(count); break;
            case StringArray.Builder b: b.Reserve(count); break;
            case BinaryArray.Builder b: b.Reserve(count); break;
            case Decimal128Array.Builder b: b.Reserve(count); break;
            case Decimal256Array.Builder b: b.Reserve(count); break;
            case TimestampArray.Builder b: b.Reserve(count); break;
            case DurationArray.Builder b: b.Reserve(count); break;
            case Date32Array.Builder b: b.Reserve(count); break;
            case Date64Array.Builder b: b.Reserve(count); break;
            case Time32Array.Builder b: b.Reserve(count); break;
            case Time64Array.Builder b: b.Reserve(count); break;
            case FixedSizeBinaryArrayBuilder b: b.Reserve(count); break;
                // List/Map/Struct manual builders don't have a native Reserve yet, 
                // but their internal lists are already pre-dimensioned in their constructors.
        }
    }

    internal abstract class TypeAccessor
    {
        public abstract IArrowArrayBuilder CreateBuilder(MemoryAllocator? allocator, CapacityInfo? capacity);
        public abstract void Append(object? value, IArrowArrayBuilder builder);
        public abstract IArrowArray Build(IArrowArrayBuilder builder);
        public virtual void CollectCapacity(object? value, CapacityInfo info) { }

        public static TypeAccessor Create(Type type)
            => Create(type, ArrowReflectionEngine.GetLogicalType(type).ArrowType);

        /// <summary>
        /// Builds an accessor for a CLR type against an already-resolved Arrow type. Callers that
        /// hold both must pass both: resolving from the Arrow type alone loses the CLR identity and
        /// silently yields the dictionary-shaped dynamic accessors.
        /// </summary>
        public static TypeAccessor Create(Type type, IArrowType arrowType)
        {
            if (arrowType is MapType mt) return new MapTypeAccessor(type, mt);
            if (arrowType is StructType st) return new StructTypeAccessor(type, st);
            if (arrowType is ListType lt) return new ListTypeAccessor(type, lt);
            return new ScalarTypeAccessor(type, arrowType);
        }

        /// <summary>
        /// Dynamic mode: the accessor of a field, whose CLR type is <see cref="ArrowTypeMap.GetClrTypeFromField"/>'s
        /// answer. There is no second table here: one written by hand took a decimal for a UUID,
        /// because Decimal128Type derives from FixedSizeBinaryType, and every timestamp for a DateTime.
        /// </summary>
        public static TypeAccessor CreateFromField(Field field) => field.DataType switch
        {
            MapType mt => new MapTypeAccessor(typeof(object), mt),
            StructType st => new DynamicStructTypeAccessor(st),
            ListType lt => new ListTypeAccessor(typeof(object), lt),
            var arrowType => new ScalarTypeAccessor(ArrowTypeMap.GetClrTypeFromField(field), arrowType),
        };
    }

    private class ScalarTypeAccessor : TypeAccessor
    {
        private readonly Type _type;
        private readonly IArrowType _arrowType;
        private readonly Action<object, IArrowArrayBuilder> _appendDelegate;

        /// <param name="type">The CLR type the column stands for.</param>
        /// <param name="arrowType">The type the batch's schema declares, which the array must carry.</param>
        public ScalarTypeAccessor(Type type, IArrowType arrowType)
        {
            _type = type;
            _arrowType = arrowType;
            _appendDelegate = CompileAppendDelegate();
        }

        private Action<object, IArrowArrayBuilder> CompileAppendDelegate()
        {
            var valueParam = Expression.Parameter(typeof(object), "value");
            var builderParam = Expression.Parameter(typeof(IArrowArrayBuilder), "builder");
            var underlyingType = Nullable.GetUnderlyingType(_type) ?? _type;

            if (underlyingType == typeof(Guid))
            {
                return (val, builder) => ((FixedSizeBinaryArrayBuilder)builder).Append(ArrowTypeMap.ToArrowUuidBytes((Guid)val));
            }
            if (underlyingType == typeof(Int128))
                return (val, builder) => ((FixedSizeBinaryArrayBuilder)builder).Append(Mapping.ArrowOpaqueCodec.Encode((Int128)val));
            if (underlyingType == typeof(UInt128))
                return (val, builder) => ((FixedSizeBinaryArrayBuilder)builder).Append(Mapping.ArrowOpaqueCodec.Encode((UInt128)val));
            if (underlyingType.IsEnum)
            {
                return (val, builder) => ((Int32Array.Builder)builder).Append(Convert.ToInt32(val));
            }
            if (underlyingType == typeof(decimal))
            {
                return _arrowType is Decimal256Type
                    ? (val, builder) => ((Decimal256Array.Builder)builder).Append(ToDecimal(val))
                    : (val, builder) => ((Decimal128Array.Builder)builder).Append(ToDecimal(val));
            }
            if (_arrowType is TimestampType)
            {
                // Either kind fits either field: an offset names an instant, and a DateTime goes
                // through the one rule that never lets the host's time zone into the value.
                return (val, builder) => ((TimestampArray.Builder)builder).Append(
                    val is DateTimeOffset offset ? offset : Mapping.TemporalNormalization.ToOffset((DateTime)val));
            }
            if (_arrowType is Date32Type)
                return (val, builder) => AppendDate((Date32Array.Builder)builder, val);
            if (_arrowType is Date64Type)
                return (val, builder) => AppendDate((Date64Array.Builder)builder, val);
#if NET6_0_OR_GREATER
            if (_arrowType is Time32Type)
                return (val, builder) => ((Time32Array.Builder)builder).Append(ToTimeOfDay(val));
            if (_arrowType is Time64Type)
                return (val, builder) => ((Time64Array.Builder)builder).Append(ToTimeOfDay(val));
#endif
            if (_arrowType is BinaryType)
                return (val, builder) => ((BinaryArray.Builder)builder).Append(((byte[])val).AsSpan());
            if (_arrowType is FixedSizeBinaryType)
            {
                // A Guid given for a bare 16-byte field writes the bytes the map writes for a UUID.
                return (val, builder) => ((FixedSizeBinaryArrayBuilder)builder).Append(
                    val is Guid guid ? ArrowTypeMap.ToArrowUuidBytes(guid) : (byte[])val);
            }
            if (underlyingType == typeof(TimeSpan))
            {
                return (val, builder) => ((DurationArray.Builder)builder).Append((TimeSpan)val);
            }

            // Numeric types: use Convert.ToXxx to safely coerce across numeric widths/kinds
            // (e.g. long→double, int→long) without the InvalidCastException that unbox-then-cast
            // throws when the boxed type differs from the target type.
            if (underlyingType == typeof(double))
                return (val, builder) => ((DoubleArray.Builder)builder).Append(Convert.ToDouble(val));
            if (underlyingType == typeof(float))
                return (val, builder) => ((FloatArray.Builder)builder).Append(Convert.ToSingle(val));
            if (underlyingType == typeof(long))
                return (val, builder) => ((Int64Array.Builder)builder).Append(Convert.ToInt64(val));
            if (underlyingType == typeof(int))
                return (val, builder) => ((Int32Array.Builder)builder).Append(Convert.ToInt32(val));
            if (underlyingType == typeof(short))
                return (val, builder) => ((Int16Array.Builder)builder).Append(Convert.ToInt16(val));
            if (underlyingType == typeof(byte))
                return (val, builder) => ((UInt8Array.Builder)builder).Append(Convert.ToByte(val));
            if (underlyingType == typeof(sbyte))
                return (val, builder) => ((Int8Array.Builder)builder).Append(Convert.ToSByte(val));
            if (underlyingType == typeof(ushort))
                return (val, builder) => ((UInt16Array.Builder)builder).Append(Convert.ToUInt16(val));
            if (underlyingType == typeof(uint))
                return (val, builder) => ((UInt32Array.Builder)builder).Append(Convert.ToUInt32(val));
            if (underlyingType == typeof(ulong))
                return (val, builder) => ((UInt64Array.Builder)builder).Append(Convert.ToUInt64(val));

            var builderType = GetBuilderType(_arrowType);
            if (builderType == null) throw new NotSupportedException($"No builder found for {_arrowType.GetType().Name}");

            var castedBuilder = Expression.Convert(builderParam, builderType);
            var castedValue = Expression.Convert(valueParam, _type);

            if (_type == typeof(string))
            {
                var stringMethod = builderType.GetMethod("Append", new[] { typeof(string), typeof(System.Text.Encoding) });
                if (stringMethod != null)
                {
                    var call = Expression.Call(castedBuilder, stringMethod, castedValue, Expression.Constant(System.Text.Encoding.UTF8));
                    return Expression.Lambda<Action<object, IArrowArrayBuilder>>(call, valueParam, builderParam).Compile();
                }
            }

            var method = builderType.GetMethods().FirstOrDefault(m => m.Name == "Append" && m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType.IsAssignableFrom(_type));
            if (method != null)
            {
                var call = Expression.Call(castedBuilder, method, castedValue);
                return Expression.Lambda<Action<object, IArrowArrayBuilder>>(call, valueParam, builderParam).Compile();
            }

            return (val, builder) =>
            {
                var m = builder.GetType().GetMethod("Append", new[] { val.GetType() });
                if (m == null) throw new NotSupportedException($"Builder {builder.GetType().Name} does not have Append({val.GetType().Name})");
                m.Invoke(builder, new[] { val });
            };
        }

        /// <summary>A decimal, or a number of another kind widened to one. A text is not a number: a parser reads it first.</summary>
        private static decimal ToDecimal(object val) => val switch
        {
            decimal d => d,
            string => throw new InvalidCastException("Unable to cast object of type 'System.String' to type 'System.Decimal'."),
            _ => Convert.ToDecimal(val)
        };

        private static void AppendDate<TUnderlying, TArray, TBuilder>(DateArrayBuilder<TUnderlying, TArray, TBuilder> builder, object val)
            where TUnderlying : struct
            where TArray : IArrowArray
            where TBuilder : class, IArrowArrayBuilder<TArray>
        {
            switch (val)
            {
#if NET6_0_OR_GREATER
                case DateOnly date: builder.Append(date); break;
#endif
                case DateTimeOffset offset: builder.Append(offset); break;
                default: builder.Append((DateTime)val); break;
            }
        }

#if NET6_0_OR_GREATER
        private static TimeOnly ToTimeOfDay(object val)
            => val is TimeSpan span ? TimeOnly.FromTimeSpan(span) : (TimeOnly)val;
#endif

        public override IArrowArrayBuilder CreateBuilder(MemoryAllocator? allocator, CapacityInfo? capacity)
        {
            IArrowArrayBuilder builder = _arrowType switch
            {
                Int8Type => new Int8Array.Builder(),
                Int16Type => new Int16Array.Builder(),
                Int32Type => new Int32Array.Builder(),
                Int64Type => new Int64Array.Builder(),
                UInt8Type => new UInt8Array.Builder(),
                UInt16Type => new UInt16Array.Builder(),
                UInt32Type => new UInt32Array.Builder(),
                UInt64Type => new UInt64Array.Builder(),
                StringType => new StringArray.Builder(),
                DoubleType => new DoubleArray.Builder(),
                FloatType => new FloatArray.Builder(),
                BooleanType => new BooleanArray.Builder(),
                // Before FixedSizeBinaryType, which both decimal types derive from.
                Decimal128Type t => new Decimal128Array.Builder(t),
                Decimal256Type t => new Decimal256Array.Builder(t),
                TimestampType t => new TimestampArray.Builder(t),
                DurationType t => new DurationArray.Builder(t),
                Date32Type => new Date32Array.Builder(),
                Date64Type => new Date64Array.Builder(),
                Time32Type t => new Time32Array.Builder(t),
                Time64Type t => new Time64Array.Builder(t),
                BinaryType => new BinaryArray.Builder(),
                FixedSizeBinaryType ft => new FixedSizeBinaryArrayBuilder(ft.ByteWidth),
                _ => throw new NotSupportedException($"Builder for {_arrowType.GetType().Name} not implemented.")
            };
            if (capacity != null && capacity.Count > 0)
            {
                ArrowSerializer.Reserve(builder, capacity.Count);
            }
            return builder;
        }

        public override void Append(object? value, IArrowArrayBuilder builder)
        {
            if (value == null) { ArrowSerializer.AppendNull(builder); return; }
            _appendDelegate(value, builder);
        }

        public override IArrowArray Build(IArrowArrayBuilder builder)
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
                Decimal128Array.Builder b => b.Build(),
                Decimal256Array.Builder b => b.Build(),
                FixedSizeBinaryArrayBuilder b => b.Build(),
                TimestampArray.Builder b => b.Build(),
                DurationArray.Builder b => b.Build(),
                Date32Array.Builder b => b.Build(),
                Date64Array.Builder b => b.Build(),
                Time32Array.Builder b => b.Build(),
                Time64Array.Builder b => b.Build(),
                _ => throw new NotSupportedException()
            };
        }

        private static Type? GetBuilderType(IArrowType type)
        {
            return type switch
            {
                Int8Type => typeof(Int8Array.Builder),
                Int16Type => typeof(Int16Array.Builder),
                Int32Type => typeof(Int32Array.Builder),
                Int64Type => typeof(Int64Array.Builder),
                UInt8Type => typeof(UInt8Array.Builder),
                UInt16Type => typeof(UInt16Array.Builder),
                UInt32Type => typeof(UInt32Array.Builder),
                UInt64Type => typeof(UInt64Array.Builder),
                StringType => typeof(StringArray.Builder),
                DoubleType => typeof(DoubleArray.Builder),
                FloatType => typeof(FloatArray.Builder),
                BooleanType => typeof(BooleanArray.Builder),
                Decimal128Type => typeof(Decimal128Array.Builder),
                TimestampType => typeof(TimestampArray.Builder),
                DurationType => typeof(DurationArray.Builder),
                FixedSizeBinaryType => typeof(FixedSizeBinaryArrayBuilder),
                _ => null
            };
        }
    }

    internal abstract class StructTypeAccessorBase : TypeAccessor
    {
        public abstract List<TypeAccessor> ChildAccessors { get; }
        public abstract List<IArrowArrayBuilder> CreateChildBuilders(MemoryAllocator? allocator, CapacityInfo? capacity);
        public abstract void AppendChildren(object value, List<IArrowArrayBuilder> builders);
    }

    internal class StructTypeAccessor : StructTypeAccessorBase
    {
        private readonly Type _type;
        private readonly StructType _structType;
        public override List<TypeAccessor> ChildAccessors { get; } = new();
        private readonly Action<object, List<IArrowArrayBuilder>> _appendChildrenDelegate;

        public StructTypeAccessor(Type type, StructType structType)
        {
            _type = type;
            _structType = structType;
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name).ToArray();
            ChildAccessors = props.Select(p => TypeAccessor.Create(p.PropertyType)).ToList();
            _appendChildrenDelegate = CompileAppendChildrenDelegate(props);
            _collectCapacityDelegate = CompileCollectCapacityDelegate(props);
        }

        private Action<object, CapacityInfo> CompileCollectCapacityDelegate(PropertyInfo[] props)
        {
            var valueParam = Expression.Parameter(typeof(object), "value");
            var infoParam = Expression.Parameter(typeof(CapacityInfo), "info");
            var castedValue = Expression.Convert(valueParam, _type);

            var body = new List<Expression>();
            for (int i = 0; i < props.Length; i++)
            {
                var prop = props[i];
                var accessor = ChildAccessors[i];
                var childInfoExpr = Expression.Call(typeof(StructTypeAccessor).GetMethod("GetOrAddChildInfo", BindingFlags.NonPublic | BindingFlags.Static)!, infoParam, Expression.Constant(prop.Name));

                var propAccess = Expression.Property(castedValue, prop);
                var collectMethod = typeof(TypeAccessor).GetMethod("CollectCapacity")!;
                body.Add(Expression.Call(Expression.Constant(accessor), collectMethod, Expression.Convert(propAccess, typeof(object)), childInfoExpr));
            }
            return Expression.Lambda<Action<object, CapacityInfo>>(Expression.Block(body), valueParam, infoParam).Compile();
        }

        internal static CapacityInfo GetOrAddChildInfo(CapacityInfo parent, string name)
        {
            if (!parent.Children.TryGetValue(name, out var child))
            {
                child = new CapacityInfo();
                parent.Children[name] = child;
            }
            return child;
        }

        private readonly Action<object, CapacityInfo> _collectCapacityDelegate;

        public override void CollectCapacity(object? value, CapacityInfo info)
        {
            if (value == null) return;
            info.Count++;
            _collectCapacityDelegate(value, info);
        }

        private Action<object, List<IArrowArrayBuilder>> CompileAppendChildrenDelegate(PropertyInfo[] props)
        {
            var valueParam = Expression.Parameter(typeof(object), "value");
            var buildersParam = Expression.Parameter(typeof(List<IArrowArrayBuilder>), "builders");
            var castedValue = Expression.Convert(valueParam, _type);

            var body = new List<Expression>();
            for (int i = 0; i < props.Length; i++)
            {
                var propAccess = Expression.Property(castedValue, props[i]);
                var builderAccess = Expression.Call(buildersParam, typeof(List<IArrowArrayBuilder>).GetMethod("get_Item")!, Expression.Constant(i));

                var childAccessor = ChildAccessors[i];
                var appendMethod = typeof(TypeAccessor).GetMethod("Append")!;
                body.Add(Expression.Call(Expression.Constant(childAccessor), appendMethod, Expression.Convert(propAccess, typeof(object)), builderAccess));
            }

            var block = Expression.Block(body);
            return Expression.Lambda<Action<object, List<IArrowArrayBuilder>>>(block, valueParam, buildersParam).Compile();
        }

        public override IArrowArrayBuilder CreateBuilder(MemoryAllocator? allocator, CapacityInfo? capacity)
        {
            var builders = new List<IArrowArrayBuilder>();
            var props = _type.GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name).ToArray();
            for (int i = 0; i < ChildAccessors.Count; i++)
            {
                var propName = props[i].Name;
                CapacityInfo? childCap = null;
                capacity?.Children.TryGetValue(propName, out childCap);
                builders.Add(ChildAccessors[i].CreateBuilder(allocator, childCap));
            }
            return new StructArrayManualBuilder(_structType, ChildAccessors, builders, this, capacity?.Count ?? 0);
        }

        public override List<IArrowArrayBuilder> CreateChildBuilders(MemoryAllocator? allocator, CapacityInfo? capacity)
        {
            var builders = new List<IArrowArrayBuilder>();
            var props = _type.GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name).ToArray();
            for (int i = 0; i < ChildAccessors.Count; i++)
            {
                CapacityInfo? childCap = null;
                capacity?.Children.TryGetValue(props[i].Name, out childCap);
                builders.Add(ChildAccessors[i].CreateBuilder(allocator, childCap));
            }
            return builders;
        }

        public override void AppendChildren(object value, List<IArrowArrayBuilder> builders)
        {
            _appendChildrenDelegate(value, builders);
        }

        public override void Append(object? value, IArrowArrayBuilder builder)
        {
            ((StructArrayManualBuilder)builder).AppendValue(value);
        }

        public override IArrowArray Build(IArrowArrayBuilder builder) => ((StructArrayManualBuilder)builder).Build();
    }


    private class ListTypeAccessor : TypeAccessor
    {
        private readonly Type _type;
        private readonly ListType _listType;
        private readonly TypeAccessor _elementAccessor;

        public ListTypeAccessor(Type type, ListType listType)
        {
            _type = type;
            _listType = listType;

            // The element accessor must keep the CLR element type when the collection is typed.
            // Resolving it from the Arrow type alone hands a struct element to the dictionary-shaped
            // DynamicStructTypeAccessor, which appends nothing for a POCO: the StructArray then
            // reports the right length over empty children, and reading it throws out of range.
            var elementType = GetElementType(type);
            _elementAccessor = elementType is not null && elementType != typeof(object)
                ? TypeAccessor.Create(elementType, listType.ValueDataType)
                : TypeAccessor.CreateFromField(listType.ValueField);
        }

        /// <summary>Element type of a typed collection; null in dynamic mode, where there is none.</summary>
        private static Type? GetElementType(Type collectionType)
        {
            if (collectionType.IsArray) return collectionType.GetElementType();
            var enumerable = collectionType.IsGenericType && collectionType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                ? collectionType
                : collectionType.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            return enumerable?.GetGenericArguments()[0];
        }

        public override void CollectCapacity(object? value, CapacityInfo info)
        {
            // A value that is no sequence is refused where it is appended.
            if (value is not IEnumerable enumerable || value is string) return;
            info.Count++;
            var elementInfo = GetOrAddChildInfo(info, "_items");
            foreach (var item in enumerable)
            {
                _elementAccessor.CollectCapacity(item, elementInfo);
            }
        }

        internal static CapacityInfo GetOrAddChildInfo(CapacityInfo parent, string name)
        {
            if (!parent.Children.TryGetValue(name, out var child))
            {
                child = new CapacityInfo();
                parent.Children[name] = child;
            }
            return child;
        }

        public override IArrowArrayBuilder CreateBuilder(MemoryAllocator? allocator, CapacityInfo? capacity)
        {
            CapacityInfo? elementCap = null;
            capacity?.Children.TryGetValue("_items", out elementCap);
            var valueBuilder = _elementAccessor.CreateBuilder(allocator, elementCap);
            return new ListArrayManualBuilder(_listType, valueBuilder, _elementAccessor, capacity?.Count ?? 0);
        }

        public override void Append(object? value, IArrowArrayBuilder builder)
        {
            ((ListArrayManualBuilder)builder).AppendValue(value);
        }

        public override IArrowArray Build(IArrowArrayBuilder builder) => ((ListArrayManualBuilder)builder).Build();
    }

    private class MapTypeAccessor : TypeAccessor
    {
        private readonly Type _type;
        private readonly MapType _mapType;
        private readonly TypeAccessor _keyAccessor;
        private readonly TypeAccessor _valueAccessor;

        public MapTypeAccessor(Type type, MapType mapType)
        {
            _type = type;
            _mapType = mapType;
            _keyAccessor = TypeAccessor.CreateFromField(mapType.KeyField);
            _valueAccessor = TypeAccessor.CreateFromField(mapType.ValueField);
        }

        public override void CollectCapacity(object? value, CapacityInfo info)
        {
            if (value == null) return;
            info.Count++;
            var dict = (IDictionary)value;
            var keyInfo = GetOrAddChildInfo(info, "_keys");
            var valInfo = GetOrAddChildInfo(info, "_values");
            foreach (DictionaryEntry entry in dict)
            {
                _keyAccessor.CollectCapacity(entry.Key, keyInfo);
                _valueAccessor.CollectCapacity(entry.Value, valInfo);
            }
        }

        internal static CapacityInfo GetOrAddChildInfo(CapacityInfo parent, string name)
        {
            if (!parent.Children.TryGetValue(name, out var child))
            {
                child = new CapacityInfo();
                parent.Children[name] = child;
            }
            return child;
        }

        public override IArrowArrayBuilder CreateBuilder(MemoryAllocator? allocator, CapacityInfo? capacity)
        {
            CapacityInfo? keyCap = null;
            CapacityInfo? valCap = null;
            capacity?.Children.TryGetValue("_keys", out keyCap);
            capacity?.Children.TryGetValue("_values", out valCap);
            var keyBuilder = _keyAccessor.CreateBuilder(allocator, keyCap);
            var valueBuilder = _valueAccessor.CreateBuilder(allocator, valCap);
            return new MapArrayManualBuilder(_mapType, keyBuilder, valueBuilder, _keyAccessor, _valueAccessor, capacity?.Count ?? 0);
        }

        public override void Append(object? value, IArrowArrayBuilder builder)
        {
            ((MapArrayManualBuilder)builder).AppendValue(value);
        }

        public override IArrowArray Build(IArrowArrayBuilder builder) => ((MapArrayManualBuilder)builder).Build();
    }

    private class ListArrayManualBuilder : IArrowArrayBuilder
    {
        private readonly ListType _type;
        private readonly IArrowArrayBuilder _valueBuilder;
        private readonly TypeAccessor _elementAccessor;
        private readonly List<int> _offsets = new() { 0 };
        private readonly List<bool> _validity = new();
        private int _nullCount;

        public ListArrayManualBuilder(ListType type, IArrowArrayBuilder valueBuilder, TypeAccessor elementAccessor, int initialCapacity)
        {
            _type = type;
            _valueBuilder = valueBuilder;
            _elementAccessor = elementAccessor;
            _offsets = new List<int>(initialCapacity + 1) { 0 };
            _validity = new List<bool>(initialCapacity);
        }

        public void AppendValue(object? value)
        {
            if (value == null) { AppendNull(); }
            else
            {
                // A text is a sequence of chars to .NET: read as one, "x" would write the list [120].
                if (value is string) throw new InvalidCastException("A text is not a list.");
                var enumerable = (IEnumerable)value;
                int count = 0;
                foreach (var item in enumerable)
                {
                    _elementAccessor.Append(item, _valueBuilder);
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
            var values = _elementAccessor.Build(_valueBuilder);
            var offsetBuf = new ArrowBuffer.Builder<int>(_offsets.Count).AppendRange(_offsets).Build();
            var validBuf = new byte[(length + 7) / 8];
            for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
            var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);
            return new ListArray(_type, length, offsetBuf, values, validBuilder.Build(), _nullCount);
        }

        public void Clear() { _validity.Clear(); _offsets.Clear(); _offsets.Add(0); _nullCount = 0; }
        public int Length => _validity.Count;
    }

    private class MapArrayManualBuilder : IArrowArrayBuilder
    {
        private readonly MapType _type;
        private readonly IArrowArrayBuilder _keyBuilder;
        private readonly IArrowArrayBuilder _valueBuilder;
        private readonly TypeAccessor _keyAccessor;
        private readonly TypeAccessor _valueAccessor;
        private readonly List<int> _offsets = new() { 0 };
        private readonly List<bool> _validity = new();
        private int _nullCount;

        public MapArrayManualBuilder(MapType type, IArrowArrayBuilder keyBuilder, IArrowArrayBuilder valueBuilder, TypeAccessor keyAccessor, TypeAccessor valueAccessor, int initialCapacity)
        {
            _type = type;
            _keyBuilder = keyBuilder;
            _valueBuilder = valueBuilder;
            _keyAccessor = keyAccessor;
            _valueAccessor = valueAccessor;
            _offsets = new List<int>(initialCapacity + 1) { 0 };
            _validity = new List<bool>(initialCapacity);
        }

        public void AppendValue(object? value)
        {
            if (value == null) { AppendNull(); }
            else
            {
                var dict = (IDictionary)value;
                int count = 0;
                foreach (DictionaryEntry entry in dict)
                {
                    _keyAccessor.Append(entry.Key, _keyBuilder);
                    _valueAccessor.Append(entry.Value, _valueBuilder);
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
            var keys = _keyAccessor.Build(_keyBuilder);
            var values = _valueAccessor.Build(_valueBuilder);
            var offsetBuf = new ArrowBuffer.Builder<int>(_offsets.Count).AppendRange(_offsets).Build();
            var validBuf = new byte[(length + 7) / 8];
            for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
            var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);

            // MapArray expects a StructArray as its items
            var structType = new StructType(new[] { _type.KeyField, _type.ValueField });
            var structArray = new StructArray(structType, keys.Length, new[] { keys, values }, ArrowBuffer.Empty, 0);

            return new MapArray(_type, length, offsetBuf, structArray, validBuilder.Build(), _nullCount);
        }

        public void Clear() { _validity.Clear(); _offsets.Clear(); _offsets.Add(0); _nullCount = 0; }
        public int Length => _validity.Count;
    }

    internal class StructArrayManualBuilder : IArrowArrayBuilder
    {
        private readonly StructType _type;
        private readonly List<TypeAccessor> _accessors;
        private readonly List<IArrowArrayBuilder> _builders;
        private readonly StructTypeAccessorBase _parentAccessor;
        private readonly List<bool> _validity = new();
        private int _nullCount;

        public StructArrayManualBuilder(StructType type, List<TypeAccessor> accessors, List<IArrowArrayBuilder> builders, StructTypeAccessorBase parentAccessor, int initialCapacity)
        {
            _type = type;
            _accessors = accessors;
            _builders = builders;
            _parentAccessor = parentAccessor;
            _validity = new List<bool>(initialCapacity);
        }

        public void AppendValue(object? value)
        {
            if (value == null) { AppendNull(); }
            else
            {
                _validity.Add(true);
                _parentAccessor.AppendChildren(value, _builders);
            }
        }

        public void AppendNull()
        {
            _validity.Add(false);
            _nullCount++;
            for (int i = 0; i < _accessors.Count; i++) ArrowSerializer.AppendNull(_builders[i]);
        }

        public IArrowArray Build()
        {
            int length = _validity.Count;
            var children = _accessors.Select((a, i) => a.Build(_builders[i])).ToList();
            var validBuf = new byte[(length + 7) / 8];
            for (int i = 0; i < length; i++) if (_validity[i]) validBuf[i / 8] |= (byte)(1 << (i % 8));
            var validBuilder = new ArrowBuffer.Builder<byte>((length + 7) / 8).AppendRange(validBuf);
            return new StructArray(_type, length, children, validBuilder.Build(), _nullCount);
        }

        public void Clear() { _validity.Clear(); _nullCount = 0; }
        public int Length => _validity.Count;
    }
}
