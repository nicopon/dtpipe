using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections;
using System.Reflection;

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
            var elementType = value?.GetType() ?? typeof(string);
            var logicalType = GetLogicalType(elementType);
            fields.Add(ArrowTypeMap.GetField(key, logicalType, true));
        }
        return fields.OrderBy(f => f.Name).ToList();
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
        // Try resolving via the central map first for primitives/scalars
        if (ArrowTypeMap.TryGetLogicalType(type, out var scalar))
            return scalar;

        if (type.IsEnum) return new ArrowTypeResult(Int32Type.Default);
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType?.IsEnum == true) return new ArrowTypeResult(Int32Type.Default);

        // Dictionaries (IDictionary<K, V>)
        if (type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(IDictionary<,>) || 
                                   type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>))))
        {
            var dictType = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IDictionary<,>) 
                ? type 
                : type.GetInterfaces().First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
            
            var keyType = dictType.GetGenericArguments()[0];
            var valueType = dictType.GetGenericArguments()[1];

            return new ArrowTypeResult(new MapType(
                ArrowTypeMap.GetField("key", GetLogicalType(keyType), false),
                ArrowTypeMap.GetField("value", GetLogicalType(valueType), true),
                false));
        }

        // Collections (IEnumerable but not string)
        if (typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string))
        {
            Type? elementType = null;
            if (type.IsArray)
            {
                elementType = type.GetElementType();
            }
            else if (type.IsGenericType)
            {
                elementType = type.GetGenericArguments()[0];
            }

            if (elementType != null)
            {
                return new ArrowTypeResult(new ListType(ArrowTypeMap.GetField("item", GetLogicalType(elementType), true)));
            }
        }

        // Structs / Nested Objects
        if (!type.IsPrimitive && !type.IsValueType || (type.IsValueType && !type.IsPrimitive && !type.IsEnum))
        {
            if (type == typeof(object) || type == typeof(System.Text.Json.Nodes.JsonObject))
            {
                 // We don't know yet, will be handled at runtime or treated as empty struct
                 return new ArrowTypeResult(new StructType(new List<Field>()));
            }
            return new ArrowTypeResult(new StructType(GetFields(type)));
        }

        throw new NotSupportedException($"Type {type.FullName} is not supported by ArrowSerializer.");
    }
}
