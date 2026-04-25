using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Serialization.Internal;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Apache.Arrow.Serialization;

public static class ArrowDeserializer
{
    private static readonly ConcurrentDictionary<Type, object> _deserializerCache = new();
    private static readonly ConcurrentDictionary<Type, Delegate> _getterCache = new();

    public static IEnumerable<T> Deserialize<T>(RecordBatch recordBatch) where T : new()
    {
        var deserializer = (TypedDeserializer<T>)_deserializerCache.GetOrAdd(typeof(T), _ => new TypedDeserializer<T>());
        return deserializer.Deserialize(recordBatch);
    }

    public static async IAsyncEnumerable<T> DeserializeAsync<T>(RecordBatch recordBatch) where T : new()
    {
        await Task.Yield();
        foreach (var item in Deserialize<T>(recordBatch))
        {
            yield return item;
        }
    }

    private static Delegate GetCompiledGetter(Type type)
    {
        return _getterCache.GetOrAdd(type, t => {
            var arrayParam = Expression.Parameter(typeof(IArrowArray), "array");
            var indexParam = Expression.Parameter(typeof(int), "index");
            var body = GetValueExpression(t, arrayParam, indexParam);
            var lambda = Expression.Lambda(typeof(Func<,,>).MakeGenericType(typeof(IArrowArray), typeof(int), t), body, arrayParam, indexParam);
            return lambda.Compile();
        });
    }

    private static Expression GetValueExpression(Type targetType, Expression arrayExpr, Expression indexExpr)
    {
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        
        // Handle Nullable wrapper
        if (targetType != underlyingType)
        {
            var isNull = Expression.Call(arrayExpr, typeof(IArrowArray).GetMethod("IsNull")!, indexExpr);
            var value = GetValueExpression(underlyingType, arrayExpr, indexExpr);
            return Expression.Condition(isNull, Expression.Constant(null, targetType), Expression.Convert(value, targetType));
        }

        var arrowType = ArrowReflectionEngine.GetLogicalType(underlyingType).ArrowType;
        var arrayType = GetArrayType(arrowType);
        var castedArray = Expression.Convert(arrayExpr, arrayType);

        if (underlyingType == typeof(Guid))
        {
            var getBytesMethod = arrayType.GetMethod("GetBytes", new[] { typeof(int) });
            var createGuidMethod = typeof(ArrowDeserializer).GetMethod("CreateGuid", BindingFlags.NonPublic | BindingFlags.Static)!;
            return Expression.Call(createGuidMethod, Expression.Call(castedArray, getBytesMethod!, indexExpr));
        }

        if (underlyingType.IsEnum)
            return Expression.Convert(Expression.Convert(Expression.Call(castedArray, typeof(Int32Array).GetMethod("GetValue", new[] { typeof(int) })!, indexExpr), typeof(int)), targetType);

        if (underlyingType == typeof(decimal))
            return Expression.Convert(Expression.Call(castedArray, typeof(Decimal128Array).GetMethod("GetValue", new[] { typeof(int) })!, indexExpr), targetType);

        if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTimeOffset))
        {
            var getMethod = typeof(TimestampArray).GetMethod("GetTimestamp", new[] { typeof(int) })!;
            var val = Expression.Call(castedArray, getMethod, indexExpr);
            if (underlyingType == typeof(DateTime))
            {
                return Expression.Convert(Expression.Property(Expression.Convert(val, typeof(DateTimeOffset)), "DateTime"), targetType);
            }
            return Expression.Convert(val, targetType);
        }

        if (underlyingType == typeof(TimeSpan))
        {
            var getMethod = typeof(DurationArray).GetMethod("GetTimeSpan", new[] { typeof(int) })!;
            return Expression.Convert(Expression.Call(castedArray, getMethod, indexExpr), targetType);
        }

        if (arrayType == typeof(StringArray))
            return Expression.Call(castedArray, arrayType.GetMethod("GetString", new[] { typeof(int), typeof(System.Text.Encoding) })!, indexExpr, Expression.Constant(System.Text.Encoding.UTF8));

        if (underlyingType.IsPrimitive)
             return Expression.Convert(Expression.Call(castedArray, arrayType.GetMethod("GetValue", new[] { typeof(int) })!, indexExpr), targetType);

        // Nested Complex Types
        if (arrowType is StructType)
        {
            var structArray = Expression.Convert(arrayExpr, typeof(StructArray));
            var bindings = new List<MemberBinding>();
            var innerProps = underlyingType.GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name).ToArray();
            
            for (int i = 0; i < innerProps.Length; i++)
            {
                var p = innerProps[i];
                var fieldsAccess = Expression.Property(structArray, "Fields");
                var fieldAccess = Expression.Call(fieldsAccess, typeof(IReadOnlyList<IArrowArray>).GetMethod("get_Item")!, Expression.Constant(i));
                var valExpr = GetValueExpression(p.PropertyType, fieldAccess, indexExpr);
                
                // If it's a class or nullable, we might need a null check for the field itself
                var isNullField = Expression.Call(fieldAccess, typeof(IArrowArray).GetMethod("IsNull")!, indexExpr);
                var assignedValue = p.PropertyType.IsValueType && Nullable.GetUnderlyingType(p.PropertyType) == null
                    ? valExpr 
                    : (Expression)Expression.Condition(isNullField, Expression.Constant(null, p.PropertyType), valExpr);

                bindings.Add(Expression.Bind(p, assignedValue));
            }

            var isNullStruct = Expression.Call(arrayExpr, typeof(IArrowArray).GetMethod("IsNull")!, indexExpr);
            var memberInit = Expression.MemberInit(Expression.New(underlyingType), bindings);
            return Expression.Condition(isNullStruct, Expression.Constant(null, underlyingType), memberInit);
        }

        if (arrowType is ListType)
        {
            Type elementType;
            string helperMethodName;
            if (underlyingType.IsArray)
            {
                elementType = underlyingType.GetElementType()!;
                helperMethodName = "BuildArray";
            }
            else if (underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(HashSet<>))
            {
                elementType = underlyingType.GetGenericArguments()[0];
                helperMethodName = "BuildHashSet";
            }
            else
            {
                elementType = (underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(List<>)) 
                    ? underlyingType.GetGenericArguments()[0] 
                    : underlyingType.GetInterfaces().First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)).GetGenericArguments()[0];
                helperMethodName = "BuildList";
            }

            var elementGetter = GetCompiledGetter(elementType);
            var helperMethod = typeof(CollectionHelper).GetMethod(helperMethodName)!.MakeGenericMethod(elementType);
            return Expression.Call(helperMethod, arrayExpr, indexExpr, Expression.Constant(elementGetter));
        }

        if (arrowType is MapType)
        {
            var dictInterface = underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(IDictionary<,>) 
                ? underlyingType 
                : underlyingType.GetInterfaces().First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
            
            var keyType = dictInterface.GetGenericArguments()[0];
            var valueType = dictInterface.GetGenericArguments()[1];
            
            var keyGetter = GetCompiledGetter(keyType);
            var valueGetter = GetCompiledGetter(valueType);
            var helperMethod = typeof(CollectionHelper).GetMethod("BuildMap")!.MakeGenericMethod(keyType, valueType);
            
            return Expression.Call(helperMethod, arrayExpr, indexExpr, Expression.Constant(keyGetter), Expression.Constant(valueGetter));
        }

        throw new NotSupportedException($"Deser of {targetType.Name} / {arrowType.GetType().Name} not supported yet.");
    }

    internal static Guid CreateGuid(ReadOnlySpan<byte> span) => new Guid(span);

    private class TypedDeserializer<T> where T : new()
    {
        private readonly Action<T, RecordBatch, int> _rowSetter;

        public TypedDeserializer()
        {
            _rowSetter = CompileRowSetter();
        }

        private Action<T, RecordBatch, int> CompileRowSetter()
        {
            var itemParam = Expression.Parameter(typeof(T), "item");
            var batchParam = Expression.Parameter(typeof(RecordBatch), "batch");
            var indexParam = Expression.Parameter(typeof(int), "index");
            var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name).ToArray();
            var body = new List<Expression>();

            for (int i = 0; i < props.Length; i++)
            {
                var prop = props[i];
                var columnAccess = Expression.Call(batchParam, typeof(RecordBatch).GetMethod("Column", new[] { typeof(int) })!, Expression.Constant(i));
                var valueAccess = GetValueExpression(prop.PropertyType, columnAccess, indexParam);
                var isNull = Expression.Call(columnAccess, typeof(IArrowArray).GetMethod("IsNull")!, indexParam);
                var assign = Expression.Call(itemParam, prop.GetSetMethod()!, valueAccess);
                body.Add(Expression.IfThen(Expression.Not(isNull), assign));
            }
            return Expression.Lambda<Action<T, RecordBatch, int>>(Expression.Block(body), itemParam, batchParam, indexParam).Compile();
        }

        public IEnumerable<T> Deserialize(RecordBatch recordBatch)
        {
            int length = recordBatch.Length;
            for (int i = 0; i < length; i++)
            {
                var item = new T();
                _rowSetter(item, recordBatch, i);
                yield return item;
            }
        }
    }

    private static Type GetArrayType(IArrowType arrowType)
    {
        return arrowType switch
        {
            Int8Type => typeof(Int8Array),
            Int16Type => typeof(Int16Array),
            Int32Type => typeof(Int32Array),
            Int64Type => typeof(Int64Array),
            UInt8Type => typeof(UInt8Array),
            UInt16Type => typeof(UInt16Array),
            UInt32Type => typeof(UInt32Array),
            UInt64Type => typeof(UInt64Array),
            StringType => typeof(StringArray),
            DoubleType => typeof(DoubleArray),
            FloatType => typeof(FloatArray),
            BooleanType => typeof(BooleanArray),
            Decimal128Type => typeof(Decimal128Array),
            TimestampType => typeof(TimestampArray),
            DurationType => typeof(DurationArray),
            FixedSizeBinaryType => typeof(FixedSizeBinaryArray),
            MapType => typeof(MapArray),
            _ => typeof(IArrowArray)
        };
    }
}
