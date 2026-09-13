using System;
using System.Text.Json;

namespace Apache.Arrow.Serialization.Mapping;

/// <summary>
/// The canonical Arrow extension metadata carried by a <see cref="Field"/>: the extension name,
/// plus the vendor and type name an <c>arrow.opaque</c> field describes itself with.
/// </summary>
public readonly struct ArrowExtensionInfo
{
    public const string NameKey = "ARROW:extension:name";
    public const string MetadataKey = "ARROW:extension:metadata";

    public string Name { get; }
    public string VendorName { get; }
    public string TypeName { get; }

    private ArrowExtensionInfo(string name, string vendorName, string typeName)
    {
        Name = name;
        VendorName = vendorName;
        TypeName = typeName;
    }

    public bool Is(string extensionName) => string.Equals(Name, extensionName, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Reads the extension metadata off a field, or returns false when it carries none.
    /// </summary>
    public static bool TryRead(Field? field, out ArrowExtensionInfo info)
    {
        info = default;
        if (field is null || !field.HasMetadata ||
            !field.Metadata.TryGetValue(NameKey, out var name) || string.IsNullOrEmpty(name))
            return false;

        var vendor = string.Empty;
        var typeName = string.Empty;

        if (field.Metadata.TryGetValue(MetadataKey, out var json) && !string.IsNullOrWhiteSpace(json))
            TryReadOpaqueMetadata(json, out vendor, out typeName);

        info = new ArrowExtensionInfo(name, vendor, typeName);
        return true;
    }

    // The payload is free-form per the Arrow spec, so a producer that writes something other than
    // the documented object leaves the names empty rather than failing the whole read.
    private static void TryReadOpaqueMetadata(string json, out string vendor, out string typeName)
    {
        vendor = string.Empty;
        typeName = string.Empty;
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Object) return;
            if (doc.RootElement.TryGetProperty("vendor_name", out var v) && v.ValueKind == JsonValueKind.String)
                vendor = v.GetString() ?? string.Empty;
            if (doc.RootElement.TryGetProperty("type_name", out var t) && t.ValueKind == JsonValueKind.String)
                typeName = t.GetString() ?? string.Empty;
        }
        catch (JsonException)
        {
        }
    }
}
