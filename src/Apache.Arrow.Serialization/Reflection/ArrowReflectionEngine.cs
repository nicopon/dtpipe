using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;

using Apache.Arrow.Serialization.Mapping;

namespace Apache.Arrow.Serialization.Reflection;

public static class ArrowReflectionEngine
{
    public static Schema GetSchema(Type type)
    {
        var fields = GetFields(type);
        if (fields.Count == 0) throw new InvalidOperationException($"Type {type.Name} has no serializable properties.");
        return new Schema(fields, null);
    }

    public static Schema GetSchema(IDictionary dict)
    {
        var fields = GetFields(dict);
        if (fields.Count == 0) throw new InvalidOperationException("Dynamic object has no properties.");
        return new Schema(fields, null);
    }

    private static List<Field> GetFields(IDictionary dict)
    {
        var fields = new List<Field>();
        foreach (DictionaryEntry entry in dict)
        {
            var key = entry.Key.ToString()!;
            var value = entry.Value;
            var logicalType = GetLogicalTypeFromValue(value);
            fields.Add(ArrowTypeMap.GetField(key, logicalType, true));
        }
        return fields.OrderBy(f => f.Name).ToList();
    }

    public static ArrowTypeResult GetLogicalTypeFromValue(object? value)
    {
        if (value == null) return new ArrowTypeResult(StringType.Default);
        
        if (value is IDictionary dict)
        {
            return new ArrowTypeResult(new StructType(GetFields(dict)));
        }

        if (value is IEnumerable enumerable && value is not string)
        {
            object? first = null;
            var it = enumerable.GetEnumerator();
            if (it.MoveNext()) first = it.Current;
            
            var elementLogicalType = GetLogicalTypeFromValue(first);
            return new ArrowTypeResult(new ListType(ArrowTypeMap.GetField("item", elementLogicalType, true)));
        }

        return GetLogicalType(value.GetType());
    }

    private static List<Field> GetFields(Type type)
    {
        var fields = new List<Field>();
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance).OrderBy(p => p.Name).ToArray();

        foreach (var prop in properties)
        {
            var logicalType = GetLogicalType(prop.PropertyType);
            bool isNullable = Nullable.GetUnderlyingType(prop.PropertyType) != null || !prop.PropertyType.IsValueType;
            fields.Add(ArrowTypeMap.GetField(prop.Name, logicalType, isNullable));
        }

        return fields;
    }

    public static ArrowTypeResult GetLogicalType(Type type)
    {
        if (type == typeof(object) || type.Name == "JsonObject")
            return new ArrowTypeResult(StringType.Default);

        if (ArrowTypeMap.TryGetLogicalType(type, out var scalar))
            return scalar;

        if (type.IsEnum) return new ArrowTypeResult(Int32Type.Default);
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType?.IsEnum == true) return new ArrowTypeResult(Int32Type.Default);

        if (type.GetInterface("IDictionary") != null)
        {
            var keyType = typeof(string);
            var valueType = typeof(object);
            var dictInterface = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition().Name.StartsWith("IDictionary"));
            
            if (dictInterface != null)
            {
                var args = dictInterface.GetGenericArguments();
                if (args.Length > 0) keyType = args[0];
                if (args.Length > 1) valueType = args[1];
            }

            return new ArrowTypeResult(new MapType(
                ArrowTypeMap.GetField("key", GetLogicalType(keyType), false),
                ArrowTypeMap.GetField("value", GetLogicalType(valueType), true),
                false));
        }

        if (type != typeof(string) && type.GetInterface("IEnumerable") != null)
        {
            Type? elementType = null;
            if (type.IsArray) elementType = type.GetElementType();
            else if (type.IsGenericType) elementType = type.GetGenericArguments()[0];
            else
            {
                var enu = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition().Name.StartsWith("IEnumerable"));
                if (enu != null) elementType = enu.GetGenericArguments()[0];
            }

            return new ArrowTypeResult(new ListType(ArrowTypeMap.GetField("item", GetLogicalType(elementType ?? typeof(object)), true)));
        }

        if (!type.IsPrimitive && !type.IsValueType || (type.IsValueType && !type.IsPrimitive && !type.IsEnum))
        {
            return new ArrowTypeResult(new StructType(GetFields(type)));
        }

        throw new NotSupportedException($"Type {type.FullName} is not supported.");
    }
}
