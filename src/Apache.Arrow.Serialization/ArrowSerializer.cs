using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Apache.Arrow.Arrays;
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
            _isDynamic = typeof(T) == typeof(object) || typeof(T).Name == "JsonObject";
            if (!_isDynamic)
            {
                _schema = ArrowReflectionEngine.GetSchema(typeof(T));
                _rootAccessor = TypeAccessor.Create(typeof(T));
            }
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

            foreach (var item in list)
            {
                rootAccessor.AppendChildren(item!, topLevelBuilders);
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
            var global = new CapacityInfo { Count = globalParentCount > 0 && sampleParentCount > 0 
                ? (int)Math.Ceiling(sample.Count * ((double)globalParentCount / sampleParentCount)) 
                : 0 };
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
            case FixedSizeBinaryArrayBuilder b: b.AppendNull(); break;
            case TimestampArray.Builder b: b.AppendNull(); break;
            case DurationArray.Builder b: b.AppendNull(); break;
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
            case TimestampArray.Builder b: b.Reserve(count); break;
            case DurationArray.Builder b: b.Reserve(count); break;
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
        {
            var arrowType = ArrowReflectionEngine.GetLogicalType(type).ArrowType;
            if (arrowType is MapType mt) return new MapTypeAccessor(type, mt);
            if (arrowType is StructType st) return new StructTypeAccessor(type, st);
            if (arrowType is ListType lt) return new ListTypeAccessor(type, lt);
            return new ScalarTypeAccessor(type);
        }

        public static TypeAccessor CreateFromArrowType(IArrowType arrowType)
        {
            // For dynamic mode, we map back to common CLR types for the scalar accessors
            var clrType = GetClrType(arrowType);
            if (arrowType is MapType mt) return new MapTypeAccessor(clrType, mt);
            if (arrowType is StructType st) return new DynamicStructTypeAccessor(st);
            if (arrowType is ListType lt) return new ListTypeAccessor(clrType, lt);
            return new ScalarTypeAccessor(clrType);
        }

        private static Type GetClrType(IArrowType type)
        {
            return type switch
            {
                Int8Type => typeof(sbyte),
                Int16Type => typeof(short),
                Int32Type => typeof(int),
                Int64Type => typeof(long),
                UInt8Type => typeof(byte),
                UInt16Type => typeof(ushort),
                UInt32Type => typeof(uint),
                UInt64Type => typeof(ulong),
                DoubleType => typeof(double),
                FloatType => typeof(float),
                StringType => typeof(string),
                BooleanType => typeof(bool),
                TimestampType => typeof(DateTime),
                DurationType => typeof(TimeSpan),
                FixedSizeBinaryType ft when ft.ByteWidth == 16 => typeof(Guid),
                Decimal128Type => typeof(decimal),
                _ => typeof(object)
            };
        }
    }

    private class ScalarTypeAccessor : TypeAccessor
    {
        private readonly Type _type;
        private readonly IArrowType _arrowType;
        private readonly Action<object, IArrowArrayBuilder> _appendDelegate;

        public ScalarTypeAccessor(Type type)
        {
            _type = type;
            _arrowType = ArrowReflectionEngine.GetLogicalType(type).ArrowType;
            _appendDelegate = CompileAppendDelegate();
        }

        private Action<object, IArrowArrayBuilder> CompileAppendDelegate()
        {
            var valueParam = Expression.Parameter(typeof(object), "value");
            var builderParam = Expression.Parameter(typeof(IArrowArrayBuilder), "builder");
            var underlyingType = Nullable.GetUnderlyingType(_type) ?? _type;

            if (underlyingType == typeof(Guid))
            {
                return (val, builder) => ((FixedSizeBinaryArrayBuilder)builder).Append(((Guid)val).ToByteArray());
            }
            if (underlyingType.IsEnum)
            {
                return (val, builder) => ((Int32Array.Builder)builder).Append(Convert.ToInt32(val));
            }
            if (underlyingType == typeof(decimal))
            {
                return (val, builder) => ((Decimal128Array.Builder)builder).Append((decimal)val);
            }
            if (underlyingType == typeof(DateTime))
            {
                return (val, builder) => ((TimestampArray.Builder)builder).Append((DateTime)val);
            }
            if (underlyingType == typeof(DateTimeOffset))
            {
                return (val, builder) => ((TimestampArray.Builder)builder).Append((DateTimeOffset)val);
            }
            if (underlyingType == typeof(TimeSpan))
            {
                return (val, builder) => ((DurationArray.Builder)builder).Append((TimeSpan)val);
            }

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

            return (val, builder) => {
                var m = builder.GetType().GetMethod("Append", new[] { val.GetType() });
                if (m == null) throw new NotSupportedException($"Builder {builder.GetType().Name} does not have Append({val.GetType().Name})");
                m.Invoke(builder, new[] { val });
            };
        }

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
                Decimal128Type t => new Decimal128Array.Builder(t),
                TimestampType t => new TimestampArray.Builder(t),
                DurationType t => new DurationArray.Builder(t),
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
                FixedSizeBinaryArrayBuilder b => b.Build(),
                TimestampArray.Builder b => b.Build(),
                DurationArray.Builder b => b.Build(),
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
            var elementType = type.IsArray ? type.GetElementType()! : type.GetGenericArguments()[0];
            _elementAccessor = TypeAccessor.Create(elementType);
        }

        public override void CollectCapacity(object? value, CapacityInfo info)
        {
            if (value == null) return;
            info.Count++;
            var enumerable = (IEnumerable)value;
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
            var dictInterface = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>) 
                ? type 
                : type.GetInterfaces().First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
            
            var keyType = dictInterface.GetGenericArguments()[0];
            var valueType = dictInterface.GetGenericArguments()[1];
            
            _keyAccessor = TypeAccessor.Create(keyType);
            _valueAccessor = TypeAccessor.Create(valueType);
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
