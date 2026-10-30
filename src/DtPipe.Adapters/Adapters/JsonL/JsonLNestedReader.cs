using System.Text.Json;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;

namespace DtPipe.Adapters.JsonL;

/// <summary>
/// How the JSONL reader reads a struct or a list column whose type the sample gave: each field, and
/// each item of a list, through the reader of its scalar type (<see cref="JsonLTypedValue"/>), so a
/// value the type does not hold is refused where it stands rather than converted by the Arrow
/// serializer. A part of the tree with no int64, double or bool leaf has no node and keeps the value as
/// the JSON gives it, as does a key the schema has no field for (the shape check names that one) and a
/// value of another kind than the container (an object for a list, a scalar for a struct).
/// </summary>
internal sealed class JsonLNestedReader
{
    private readonly Type? _leafType;
    private readonly Func<JsonElement, object?>? _leaf;
    private readonly Dictionary<string, JsonLNestedReader>? _fields;
    private readonly JsonLNestedReader? _item;

    private JsonLNestedReader(Type? leafType, Func<JsonElement, object?>? leaf,
        Dictionary<string, JsonLNestedReader>? fields, JsonLNestedReader? item)
    {
        _leafType = leafType;
        _leaf = leaf;
        _fields = fields;
        _item = item;
    }

    /// <summary>The reader of a column of this Arrow type, or null when no value in it needs one.</summary>
    internal static JsonLNestedReader? Of(IArrowType type)
    {
        switch (type)
        {
            case StructType structType:
                Dictionary<string, JsonLNestedReader>? fields = null;
                foreach (var field in structType.Fields)
                {
                    if (Of(field.DataType) is { } inner)
                        (fields ??= new Dictionary<string, JsonLNestedReader>(StringComparer.Ordinal))[field.Name] = inner;
                }
                return fields == null ? null : new JsonLNestedReader(null, null, fields, null);
            case ListType listType:
                return Of(listType.ValueDataType) is { } item ? new JsonLNestedReader(null, null, null, item) : null;
            default:
                var clrType = type switch
                {
                    Int64Type => typeof(long),
                    DoubleType => typeof(double),
                    BooleanType => typeof(bool),
                    _ => null
                };
                return clrType != null && JsonLTypedValue.BuildReader(clrType) is { } read
                    ? new JsonLNestedReader(clrType, read, null, null)
                    : null;
        }
    }

    /// <summary>
    /// The value read into the type, or <see cref="TypedTextCell.Unreadable"/> when a leaf of it does not
    /// read; <see cref="FirstUnreadable"/> then says which.
    /// </summary>
    internal object? Read(JsonElement element)
    {
        if (_leaf != null) return _leaf(element);

        if (element.ValueKind == JsonValueKind.Null) return null;

        if (_fields != null && element.ValueKind == JsonValueKind.Object)
        {
            var dict = new Dictionary<string, object?>();
            foreach (var prop in element.EnumerateObject())
            {
                object? value;
                if (_fields.TryGetValue(prop.Name, out var field))
                {
                    value = field.Read(prop.Value);
                    if (ReferenceEquals(value, TypedTextCell.Unreadable)) return value;
                }
                else
                {
                    value = JsonLStreamReader.ToValue(prop.Value);
                }
                dict.Add(prop.Name, value);
            }
            return dict;
        }

        if (_item != null && element.ValueKind == JsonValueKind.Array)
        {
            var list = new List<object?>(element.GetArrayLength());
            foreach (var item in element.EnumerateArray())
            {
                var value = _item.Read(item);
                if (ReferenceEquals(value, TypedTextCell.Unreadable)) return value;
                list.Add(value);
            }
            return list;
        }

        return JsonLStreamReader.ToValue(element);
    }

    /// <summary>
    /// Where the first value <see cref="Read"/> refuses stands: its path from <paramref name="path"/> (the
    /// column), a field after a dot and a list item as <c>[]</c>, with the type it should have read.
    /// Runs once, for the message.
    /// </summary>
    internal (string Path, Type Type)? FirstUnreadable(JsonElement element, string path)
    {
        if (_leaf != null)
            return ReferenceEquals(_leaf(element), TypedTextCell.Unreadable) ? (path, _leafType!) : null;

        if (_fields != null && element.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                if (_fields.TryGetValue(prop.Name, out var field)
                    && field.FirstUnreadable(prop.Value, path + "." + prop.Name) is { } found)
                    return found;
            }
        }
        else if (_item != null && element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
            {
                if (_item.FirstUnreadable(item, path + "[]") is { } found) return found;
            }
        }
        return null;
    }
}
