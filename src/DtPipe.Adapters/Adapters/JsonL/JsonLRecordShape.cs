using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Adapters.JsonL;

/// <summary>
/// The keys a schema has a column for, as a tree, so a reader can tell in one walk whether a record
/// carries a key the schema would drop. Only keys count: a value of another type than the schema's is
/// not this check's business.
/// </summary>
internal sealed class JsonLRecordShape
{
    private static readonly JsonLRecordShape Leaf = new(null, null);

    private readonly Dictionary<string, JsonLRecordShape>? _members;
    private readonly JsonLRecordShape? _element;

    private JsonLRecordShape(Dictionary<string, JsonLRecordShape>? members, JsonLRecordShape? element)
    {
        _members = members;
        _element = element;
    }

    public static JsonLRecordShape Of(Schema schema)
    {
        // Top-level names match regardless of case, as schema discovery merges them.
        var members = new Dictionary<string, JsonLRecordShape>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in schema.FieldsList)
            members[field.Name] = Of(field.DataType);
        return new JsonLRecordShape(members, null);
    }

    private static JsonLRecordShape Of(IArrowType type)
    {
        switch (type)
        {
            case StructType structType:
                var members = new Dictionary<string, JsonLRecordShape>(StringComparer.Ordinal);
                foreach (var field in structType.Fields)
                    members[field.Name] = Of(field.DataType);
                return new JsonLRecordShape(members, null);
            case ListType listType:
                return new JsonLRecordShape(null, Of(listType.ValueDataType));
            case LargeListType largeListType:
                return new JsonLRecordShape(null, Of(largeListType.ValueDataType));
            default:
                return Leaf;
        }
    }

    /// <summary>The dotted path of the first key of the record the schema has no column for, or null.</summary>
    public string? FirstUnknownKey(Dictionary<string, object?> record) => FindUnknown(record);

    // Runs for every record: the path is put together on the way back up, only when a key is found.
    private string? FindUnknown(object? value)
    {
        if (_members != null && value is Dictionary<string, object?> dictionary)
        {
            foreach (var entry in dictionary)
            {
                if (!_members.TryGetValue(entry.Key, out var member))
                {
                    // Null and absent are one thing in a column: a null that is not written loses nothing.
                    if (entry.Value is null) continue;
                    return entry.Key;
                }

                var inner = member.FindUnknown(entry.Value);
                if (inner != null) return inner.StartsWith('[') ? entry.Key + inner : entry.Key + "." + inner;
            }
        }
        else if (_element != null && value is List<object?> list)
        {
            foreach (var item in list)
            {
                var inner = _element.FindUnknown(item);
                if (inner != null) return "[]." + inner;
            }
        }
        return null;
    }
}
