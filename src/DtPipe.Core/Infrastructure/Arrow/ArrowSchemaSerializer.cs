using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Serializes and deserializes an Apache Arrow <see cref="Schema"/> to/from a compact,
/// human-readable JSON representation. Handles the full type tree (scalars, nested
/// structs, lists, maps, timestamps with timezone, decimals, fixed-size binaries, …).
///
/// Type name encoding (the "type" string in JSON):
///   Scalars  : "utf8", "binary", "largeutf8", "largebinary", "bool", "null"
///              "int8/16/32/64", "uint8/16/32/64", "float16/32/64"
///              "date32", "date64"
///   Parameterised: "fixedsizebinary:16", "decimal128:38:18", "decimal256:76:18"
///                  "timestamp:us", "timestamp:us:UTC"
///                  "time32:ms", "time64:ns", "duration:us"
///   Nested   : "list", "largelist", "struct", "map"  — child fields in "children"
/// </summary>
public static class ArrowSchemaSerializer
{
    private static readonly JsonSerializerOptions _pretty  = new() { WriteIndented = true };
    private static readonly JsonSerializerOptions _compact = new() { WriteIndented = false };

    // ── Public API ───────────────────────────────────────────────────────────

    /// <summary>Pretty-printed JSON — for .dtschema files.</summary>
    public static string SerializePretty(Schema schema)
        => BuildSchemaNode(schema).ToJsonString(_pretty);

    /// <summary>Compact single-line JSON — for YAML ProviderOptions embedding.</summary>
    public static string SerializeCompact(Schema schema)
        => BuildSchemaNode(schema).ToJsonString(_compact);

    /// <summary>Reconstructs an Arrow <see cref="Schema"/> from either pretty or compact JSON.</summary>
    public static Schema Deserialize(string json)
    {
        var root = JsonNode.Parse(json)
            ?? throw new InvalidOperationException("Schema JSON is null.");
        return ParseSchema(root);
    }

    // ── Serialization ────────────────────────────────────────────────────────

    private static JsonObject BuildSchemaNode(Schema schema)
    {
        var node = new JsonObject();
        var fieldsArr = new JsonArray();
        foreach (var field in schema.FieldsList)
            fieldsArr.Add(FieldToNode(field));
        node["fields"] = fieldsArr;
        if (schema.Metadata?.Count > 0)
            node["metadata"] = MetadataToNode(schema.Metadata);
        return node;
    }

    private static JsonObject FieldToNode(Field field)
    {
        var node = new JsonObject();
        node["name"]     = field.Name;
        node["nullable"] = field.IsNullable;

        var (typeName, children) = TypeToString(field.DataType);
        node["type"] = typeName;

        if (children is { Count: > 0 })
        {
            var arr = new JsonArray();
            foreach (var c in children) arr.Add(c);
            node["children"] = arr;
        }

        if (field.Metadata?.Count > 0)
            node["metadata"] = MetadataToNode(field.Metadata);

        return node;
    }

    /// <summary>Returns (typeString, childFieldNodes?) for an Arrow type.</summary>
    private static (string type, List<JsonObject>? children) TypeToString(IArrowType t)
    {
        // CRITICAL: Decimal128/256 inherit from FixedSizeBinaryType — check them first.
        if (t is Decimal128Type d128) return ($"decimal128:{d128.Precision}:{d128.Scale}", null);
        if (t is Decimal256Type d256) return ($"decimal256:{d256.Precision}:{d256.Scale}", null);
        if (t is FixedSizeBinaryType fsb) return ($"fixedsizebinary:{fsb.ByteWidth}", null);

        if (t is TimestampType ts)
        {
            var u = UnitStr(ts.Unit);
            return string.IsNullOrEmpty(ts.Timezone)
                ? ($"timestamp:{u}", null)
                : ($"timestamp:{u}:{ts.Timezone}", null);
        }
        if (t is Time32Type t32)  return ($"time32:{UnitStr(t32.Unit)}", null);
        if (t is Time64Type t64)  return ($"time64:{UnitStr(t64.Unit)}", null);
        if (t is DurationType dur) return ($"duration:{UnitStr(dur.Unit)}", null);

        if (t is ListType lst)
        {
            // ValueDataType gives the element type; name it "item" (Arrow convention).
            var childField = new Field("item", lst.ValueDataType, true);
            return ("list", new List<JsonObject> { FieldToNode(childField) });
        }
        if (t is LargeListType llst)
        {
            var childField = new Field("item", llst.ValueDataType, true);
            return ("largelist", new List<JsonObject> { FieldToNode(childField) });
        }
        if (t is StructType st)
        {
            var children = st.Fields.Select(FieldToNode).ToList();
            return ("struct", children);
        }
        if (t is MapType mt)
        {
            // Map children: [keyField, valueField] (Arrow convention)
            return ("map", new List<JsonObject> { FieldToNode(mt.KeyField), FieldToNode(mt.ValueField) });
        }

        return t switch
        {
            BooleanType    => ("bool",        null),
            Int8Type       => ("int8",        null),
            Int16Type      => ("int16",       null),
            Int32Type      => ("int32",       null),
            Int64Type      => ("int64",       null),
            UInt8Type      => ("uint8",       null),
            UInt16Type     => ("uint16",      null),
            UInt32Type     => ("uint32",      null),
            UInt64Type     => ("uint64",      null),
            HalfFloatType  => ("float16",     null),
            FloatType      => ("float32",     null),
            DoubleType     => ("float64",     null),
            StringType     => ("utf8",        null),
            LargeStringType => ("largeutf8",  null),
            BinaryType     => ("binary",      null),
            LargeBinaryType => ("largebinary",null),
            Date32Type     => ("date32",      null),
            Date64Type     => ("date64",      null),
            NullType       => ("null",        null),
            _              => ("utf8",        null)   // safe fallback
        };
    }

    private static JsonObject MetadataToNode(IReadOnlyDictionary<string, string> meta)
    {
        var obj = new JsonObject();
        foreach (var kv in meta) obj[kv.Key] = kv.Value;
        return obj;
    }

    // ── Deserialization ──────────────────────────────────────────────────────

    private static Schema ParseSchema(JsonNode root)
    {
        var fields = new List<Field>();
        if (root["fields"] is JsonArray arr)
            foreach (var item in arr)
                if (item != null) fields.Add(ParseField(item));

        IReadOnlyDictionary<string, string>? meta = ParseMetadata(root["metadata"]);
        return new Schema(fields, meta);
    }

    private static Field ParseField(JsonNode node)
    {
        var name     = node["name"]?.GetValue<string>() ?? "";
        var nullable = node["nullable"]?.GetValue<bool>() ?? true;
        var typeName = node["type"]?.GetValue<string>() ?? "utf8";

        var children = new List<Field>();
        if (node["children"] is JsonArray arr)
            foreach (var item in arr)
                if (item != null) children.Add(ParseField(item));

        var arrowType = ParseArrowType(typeName, children);
        var meta      = ParseMetadata(node["metadata"]);

        return meta?.Count > 0
            ? new Field(name, arrowType, nullable, meta)
            : new Field(name, arrowType, nullable);
    }

    private static IArrowType ParseArrowType(string typeName, List<Field> children)
    {
        if (typeName.StartsWith("decimal128:", StringComparison.Ordinal))
        {
            var p = typeName.Split(':');
            return new Decimal128Type(int.Parse(p[1]), int.Parse(p[2]));
        }
        if (typeName.StartsWith("decimal256:", StringComparison.Ordinal))
        {
            var p = typeName.Split(':');
            return new Decimal256Type(int.Parse(p[1]), int.Parse(p[2]));
        }
        if (typeName.StartsWith("fixedsizebinary:", StringComparison.Ordinal))
        {
            return new FixedSizeBinaryType(int.Parse(typeName.Split(':')[1]));
        }
        if (typeName.StartsWith("timestamp:", StringComparison.Ordinal))
        {
            var p  = typeName.Split(':');
            var u  = ParseUnit(p[1]);
            var tz = p.Length > 2 ? string.Join(":", p[2..]) : null; // handle "America/New_York"
            return new TimestampType(u, tz);
        }
        if (typeName.StartsWith("time32:", StringComparison.Ordinal))
            return new Time32Type(ParseUnit(typeName.Split(':')[1]));
        if (typeName.StartsWith("time64:", StringComparison.Ordinal))
            return new Time64Type(ParseUnit(typeName.Split(':')[1]));
        if (typeName.StartsWith("duration:", StringComparison.Ordinal))
        {
            return ParseUnit(typeName.Split(':')[1]) switch
            {
                TimeUnit.Second      => DurationType.Second,
                TimeUnit.Millisecond => DurationType.Millisecond,
                TimeUnit.Nanosecond  => DurationType.Nanosecond,
                _                    => DurationType.Microsecond
            };
        }

        return typeName switch
        {
            "bool"        => BooleanType.Default,
            "int8"        => Int8Type.Default,
            "int16"       => Int16Type.Default,
            "int32"       => Int32Type.Default,
            "int64"       => Int64Type.Default,
            "uint8"       => UInt8Type.Default,
            "uint16"      => UInt16Type.Default,
            "uint32"      => UInt32Type.Default,
            "uint64"      => UInt64Type.Default,
            "float16"     => HalfFloatType.Default,
            "float32"     => FloatType.Default,
            "float64"     => DoubleType.Default,
            "utf8"        => StringType.Default,
            "largeutf8"   => LargeStringType.Default,
            "binary"      => BinaryType.Default,
            "largebinary" => LargeBinaryType.Default,
            "date32"      => Date32Type.Default,
            "date64"      => Date64Type.Default,
            "null"        => NullType.Default,
            // Use the IArrowType constructor (not Field) to match the path taken by InferListType,
            // so ArrowSerializer.SerializeAsync sees the same type structure.
            "list"        => children.Count > 0
                                 ? new ListType(children[0].DataType)
                                 : new ListType(StringType.Default),
            "largelist"   => children.Count > 0
                                 ? new LargeListType(children[0].DataType)
                                 : new LargeListType(StringType.Default),
            "struct"      => new StructType(children),
            "map"         => children.Count >= 2
                                 ? new MapType(children[0], children[1])
                                 : new MapType(StringType.Default, StringType.Default),
            _             => StringType.Default
        };
    }

    private static IReadOnlyDictionary<string, string>? ParseMetadata(JsonNode? node)
    {
        if (node is not JsonObject obj || obj.Count == 0) return null;
        return obj.ToDictionary(kv => kv.Key, kv => kv.Value?.GetValue<string>() ?? "");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static string UnitStr(TimeUnit u) => u switch
    {
        TimeUnit.Second      => "s",
        TimeUnit.Millisecond => "ms",
        TimeUnit.Microsecond => "us",
        TimeUnit.Nanosecond  => "ns",
        _                    => "us"
    };

    private static TimeUnit ParseUnit(string s) => s switch
    {
        "s"  => TimeUnit.Second,
        "ms" => TimeUnit.Millisecond,
        "us" => TimeUnit.Microsecond,
        "ns" => TimeUnit.Nanosecond,
        _    => TimeUnit.Microsecond
    };
}
