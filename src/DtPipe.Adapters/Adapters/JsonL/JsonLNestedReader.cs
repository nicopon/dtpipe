using System.Text.Json;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;

namespace DtPipe.Adapters.JsonL;

/// <summary>
/// How the JSONL reader reads a struct or a list column whose type the sample or the schema gave: each
/// field, and each item of a list, through the reader of its scalar type (<see cref="JsonLTypedValue"/>),
/// so a value the type does not hold is refused where it stands rather than converted by the Arrow
/// serializer. A value of another kind than its container is refused too: read as a sequence, a text
/// would write a list of its bytes. The exception is a struct with a <c>_value</c> field, which schema
/// discovery adds when the sample holds a scalar beside an object: the scalar is that field's value. A
/// leaf with no scalar reader keeps the value as the JSON gives it, as does a key the schema has no field
/// for (the shape check names that one).
/// </summary>
internal sealed class JsonLNestedReader
{
    private const string ScalarField = "_value";

    private readonly Type? _leafType;
    private readonly Func<JsonElement, object?>? _leaf;
    private readonly bool _isStruct;
    private readonly bool _holdsScalar;
    private readonly Dictionary<string, JsonLNestedReader>? _fields;
    private readonly JsonLNestedReader? _item;

    private JsonLNestedReader(Type? leafType, Func<JsonElement, object?>? leaf, bool isStruct, bool holdsScalar,
        Dictionary<string, JsonLNestedReader>? fields, JsonLNestedReader? item)
    {
        _leafType = leafType;
        _leaf = leaf;
        _isStruct = isStruct;
        _holdsScalar = holdsScalar;
        _fields = fields;
        _item = item;
    }

    /// <summary>The reader of a column of this Arrow type, or null for a scalar no reader reads.</summary>
    internal static JsonLNestedReader? Of(IArrowType type)
    {
        switch (type)
        {
            case StructType structType:
                var fields = new Dictionary<string, JsonLNestedReader>(StringComparer.Ordinal);
                foreach (var field in structType.Fields)
                {
                    if (Of(field.DataType) is { } inner)
                        fields[field.Name] = inner;
                }
                return new JsonLNestedReader(null, null, isStruct: true,
                    holdsScalar: structType.Fields.Any(f => f.Name == ScalarField), fields, null);
            case ListType listType:
                return new JsonLNestedReader(null, null, isStruct: false, holdsScalar: false, null, Of(listType.ValueDataType));
            default:
                var clrType = type switch
                {
                    Int64Type => typeof(long),
                    DoubleType => typeof(double),
                    BooleanType => typeof(bool),
                    Decimal128Type => typeof(decimal),
                    _ => null
                };
                return clrType != null && JsonLTypedValue.BuildReader(clrType) is { } read
                    ? new JsonLNestedReader(clrType, read, isStruct: false, holdsScalar: false, null, null)
                    : null;
        }
    }

    /// <summary>
    /// The value read into the type, or <see cref="TypedTextCell.Unreadable"/> when it, or a leaf of it,
    /// does not read; <see cref="FirstUnreadable"/> then says which.
    /// </summary>
    internal object? Read(JsonElement element)
    {
        if (_leaf != null) return _leaf(element);

        if (element.ValueKind == JsonValueKind.Null) return null;

        return _isStruct ? ReadStruct(element) : ReadList(element);
    }

    private object? ReadStruct(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            if (!_holdsScalar) return TypedTextCell.Unreadable;

            var scalar = ReadOf(_fields!.GetValueOrDefault(ScalarField), element);
            return ReferenceEquals(scalar, TypedTextCell.Unreadable)
                ? scalar
                : new Dictionary<string, object?> { [ScalarField] = scalar };
        }

        var dict = new Dictionary<string, object?>();
        foreach (var prop in element.EnumerateObject())
        {
            var value = ReadOf(_fields!.GetValueOrDefault(prop.Name), prop.Value);
            if (ReferenceEquals(value, TypedTextCell.Unreadable)) return value;
            dict.Add(prop.Name, value);
        }
        return dict;
    }

    private object? ReadList(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Array) return TypedTextCell.Unreadable;

        var list = new List<object?>(element.GetArrayLength());
        foreach (var item in element.EnumerateArray())
        {
            var value = ReadOf(_item, item);
            if (ReferenceEquals(value, TypedTextCell.Unreadable)) return value;
            list.Add(value);
        }
        return list;
    }

    private static object? ReadOf(JsonLNestedReader? reader, JsonElement element)
        => reader != null ? reader.Read(element) : JsonLStreamReader.ToValue(element);

    /// <summary>
    /// Where the first value <see cref="Read"/> refuses stands: its path from <paramref name="path"/> (the
    /// column), a field after a dot and a list item as <c>[]</c>, with what it should have read as. Runs
    /// once, for the message.
    /// </summary>
    internal (string Path, string Expected)? FirstUnreadable(JsonElement element, string path)
    {
        if (_leaf != null)
        {
            return ReferenceEquals(_leaf(element), TypedTextCell.Unreadable)
                ? (path, DtPipe.Core.Helpers.TypeHelper.HintFor(_leafType!) ?? _leafType!.Name)
                : null;
        }

        if (element.ValueKind == JsonValueKind.Null) return null;

        if (_isStruct)
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                if (!_holdsScalar) return (path, "a struct");
                return _fields!.GetValueOrDefault(ScalarField)?.FirstUnreadable(element, path + "." + ScalarField);
            }

            foreach (var prop in element.EnumerateObject())
            {
                if (_fields!.TryGetValue(prop.Name, out var field)
                    && field.FirstUnreadable(prop.Value, path + "." + prop.Name) is { } found)
                    return found;
            }
            return null;
        }

        if (element.ValueKind != JsonValueKind.Array) return (path, "a list");

        if (_item != null)
        {
            foreach (var item in element.EnumerateArray())
            {
                if (_item.FirstUnreadable(item, path + "[]") is { } found) return found;
            }
        }
        return null;
    }
}
