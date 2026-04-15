namespace DtPipe.Adapters.Xml;

internal static class XmlMetadata
{
    public const string ComponentName = "xml";
    public static bool CanHandle(string connectionString) => connectionString.EndsWith(".xml", StringComparison.OrdinalIgnoreCase);
    public const bool SupportsStdio = true;
}
