using System.Globalization;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Core.Models;

using DtPipe.Adapters.Common;
using DtPipe.Core.Helpers;

namespace DtPipe.Adapters.Xml;

/// <summary>
/// F15 — static type-inference helpers extracted from XmlStreamReader.
/// </summary>
internal static class XmlTypeInferrer
{
    internal static Type GetSimplifiedClrType(Field f)
    {
        return f.DataType switch
        {
            ListType or LargeListType or StructType or MapType => typeof(object),
            _ => ArrowTypeMapper.GetClrTypeFromField(f)
        };
    }
    /// <summary>
    /// What a hint names, read off <see cref="ArrowTypeMap"/>: a second table here lost the zone of a
    /// <c>datetimeoffset</c> and the extension of a <c>uuid</c>, and named a zoned timestamp for a
    /// <c>datetime</c>.
    /// </summary>
    internal static ArrowTypeResult ResolveHintToLogicalType(string hint)
        => LogicalTypeOf(ResolveHintToClrType(hint) ?? typeof(string));

    internal static ArrowTypeResult LogicalTypeOf(Type clrType) => ArrowTypeMapper.GetLogicalType(clrType);

    internal static Dictionary<string, Type> ParseColumnTypesOption(string spec)
    {
        var result = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(spec)) return result;

        foreach (var entry in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var idx = entry.IndexOf(':');
            if (idx <= 0) continue;
            var name = entry[..idx].Trim();
            var typeName = entry[(idx + 1)..].Trim();
            result[name] = TypeHelper.RequireTypeHint("--column-types", name, typeName);
        }
        return result;
    }
    /// <summary>A nullable field carrying the mapping's metadata: a UUID's extension, a decimal's undeclared mark.</summary>
    internal static Field FieldFor(string name, ArrowTypeResult logical) => ArrowTypeMap.GetField(name, logical);

    internal static Type? ResolveHintToClrType(string hint) => TypeHelper.ParseTypeHint(hint);
    /// <summary>
    /// Adds the parser of each column of <paramref name="types"/>, keyed by its dot path, keeping any
    /// the map already holds: a declared type wins over an inferred one. A string column has none: its
    /// text stays text. Every other one reads a non-empty cell under <see cref="TypedTextCell"/>'s rule.
    /// </summary>
    internal static void AddColumnParsers(Dictionary<string, XmlTypedColumn> parsers, IEnumerable<KeyValuePair<string, Type>> types, int? sampledRows)
    {
        foreach (var kvp in types)
        {
            if (!parsers.ContainsKey(kvp.Key) && BuildParser(kvp.Value) is { } parser)
                parsers[kvp.Key] = new XmlTypedColumn(parser, kvp.Value, sampledRows);
        }
    }

    /// <summary>The same for the columns of a schema, whose decimals read at the width it declares.</summary>
    internal static void AddColumnParsers(Dictionary<string, XmlTypedColumn> parsers, IEnumerable<PipeColumnInfo> columns)
    {
        foreach (var column in columns)
        {
            if (!parsers.ContainsKey(column.Name) && BuildParser(column.ClrType, DecimalWidth.Of(column.Precision, column.Scale)) is { } parser)
                parsers[column.Name] = new XmlTypedColumn(parser, column.ClrType, null);
        }
    }

    /// <summary>
    /// The parser of a typed column, or null for a text column. It returns
    /// <see cref="TypedTextCell.Unreadable"/> for a text it does not read.
    /// </summary>
    internal static Func<string, object?>? BuildParser(Type clrType, DecimalWidth? width = null)
    {
        if (clrType == typeof(Guid))
            return static s => Guid.TryParse(s, out var g) ? g : TypedTextCell.Unreadable;

        if (clrType == typeof(int))
            return static s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : TypedTextCell.Unreadable;

        if (clrType == typeof(long))
            return static s => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : TypedTextCell.Unreadable;

        if (clrType == typeof(double))
            return static s => TextNumber.TryReadDouble(s, out var v) ? v : TypedTextCell.Unreadable;

        if (clrType == typeof(float))
            return static s => TextNumber.TryReadFloat(s, out var v) ? v : TypedTextCell.Unreadable;

        if (clrType == typeof(decimal))
        {
            var column = width ?? DecimalWidth.Undeclared;
            return s => TextNumber.TryReadDecimal(s, out var v) && column.TryRead(v, out var read) ? read : TypedTextCell.Unreadable;
        }

        if (clrType == typeof(bool))
            return static s =>
            {
                if (bool.TryParse(s, out var b)) return b;
                return s.ToLowerInvariant() switch { "1" or "yes" or "true" => true, "0" or "no" or "false" => false, _ => TypedTextCell.Unreadable };
            };

        if (clrType == typeof(DateTime))
            return TypedTextCell.ReadDateTime;

        if (clrType == typeof(DateTimeOffset))
            return TypedTextCell.ReadDateTimeOffset;

        if (clrType == typeof(DateOnly))
            return TypedTextCell.ReadDate;

        if (clrType == typeof(TimeOnly))
            return TypedTextCell.ReadTime;

        // A string column, or a type no parser reads: the cell stays text.
        return null;
    }
    internal static string? InferTypeHint(List<string> samples)
    {
        if (samples.Count == 0) return null;

        bool allMatch(Func<string, bool> test) => samples.All(test);

        if (allMatch(s => Guid.TryParse(s, out _))) return "uuid";

        if (allMatch(s => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)))
        {
            return samples.All(s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)) ? "int32" : "int64";
        }

        if (TextNumber.InferHint(samples) is { } number) return number;

        if (allMatch(s => bool.TryParse(s, out _) || s.ToLowerInvariant() is "0" or "1" or "yes" or "no" or "true" or "false")) return "bool";

        if (allMatch(TypedTextCell.ReadsAsDate)) return "datetime";

        return null;
    }
}

/// <summary>
/// How a typed XML column reads its text: its parser, its type, and the sample the type was inferred
/// from, null when the type was declared.
/// </summary>
internal sealed record XmlTypedColumn(Func<string, object?> Parser, Type Type, int? SampledRows);
