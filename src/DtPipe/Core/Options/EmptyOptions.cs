using DtPipe.Core.Options;

namespace DtPipe.Core.Options;

/// <summary>
/// Placeholder options for components that do not require specific configuration.
/// </summary>
public class EmptyOptions : IProviderOptions
{
    public static string Prefix => string.Empty;
    public static string DisplayName => "Empty Options";
}
