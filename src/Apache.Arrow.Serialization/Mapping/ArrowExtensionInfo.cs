using System;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Apache.Arrow.Serialization.Mapping;

/// <summary>
/// The canonical Arrow extension metadata carried by a <see cref="Field"/>: the extension name,
/// plus the vendor and type name an <c>arrow.opaque</c> field describes itself with.
/// </summary>
/// <remarks>
/// Reading the name must stay cheap: <see cref="ArrowTypeMap.GetValue"/> asks once per cell, on
/// the hottest path in the product. So the name comes from a dictionary lookup and nothing else,
/// and the opaque payload — which is JSON, and costs a parse — is read only when a caller reaches
/// for <see cref="VendorName"/> or <see cref="TypeName"/>, then cached against the field.
/// </remarks>
public readonly struct ArrowExtensionInfo
{
    public const string NameKey = "ARROW:extension:name";
    public const string MetadataKey = "ARROW:extension:metadata";

    private sealed class OpaqueNames
    {
        public string Vendor = string.Empty;
        public string Type = string.Empty;
    }

    private static readonly ConditionalWeakTable<Field, OpaqueNames> OpaqueCache = new();

    private readonly Field _field;

    public string Name { get; }

    private ArrowExtensionInfo(string name, Field field)
    {
        Name = name;
        _field = field;
    }

    public bool Is(string extensionName) => string.Equals(Name, extensionName, StringComparison.OrdinalIgnoreCase);

    public string VendorName => Opaque().Vendor;
    public string TypeName => Opaque().Type;

    private OpaqueNames Opaque()
        => _field is null ? new OpaqueNames() : OpaqueCache.GetValue(_field, ParseOpaque);

    /// <summary>
    /// Reads the extension name off a field, or returns false when it carries none.
    /// </summary>
    public static bool TryRead(Field? field, out ArrowExtensionInfo info)
    {
        info = default;
        if (field is null || !field.HasMetadata ||
            !field.Metadata.TryGetValue(NameKey, out var name) || string.IsNullOrEmpty(name))
            return false;

        info = new ArrowExtensionInfo(name, field);
        return true;
    }

    // The payload is free-form per the Arrow spec, so a producer that writes something other than
    // the documented object leaves the names empty rather than failing the whole read.
    private static OpaqueNames ParseOpaque(Field field)
    {
        var names = new OpaqueNames();
        if (!field.HasMetadata || !field.Metadata.TryGetValue(MetadataKey, out var json) ||
            string.IsNullOrWhiteSpace(json))
            return names;

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Object) return names;
            if (doc.RootElement.TryGetProperty("vendor_name", out var v) && v.ValueKind == JsonValueKind.String)
                names.Vendor = v.GetString() ?? string.Empty;
            if (doc.RootElement.TryGetProperty("type_name", out var t) && t.ValueKind == JsonValueKind.String)
                names.Type = t.GetString() ?? string.Empty;
        }
        catch (JsonException)
        {
        }
        return names;
    }
}
