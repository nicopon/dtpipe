using DtPipe.Core.Options;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Records that the source query was built from <c>--table</c> rather than typed by the user, and
/// what it was built from.
/// </summary>
/// <remarks>
/// The driver's own message names the relation it could not find and stops there, which reads as
/// though the user had written that name. Carrying the provenance this far lets the failure say
/// where the name came from and how to take the query over.
/// </remarks>
public class AutoBuiltSourceQuery : IOptionSet
{
    public static string Prefix => "auto-query";
    public static string DisplayName => "Auto-built Source Query";

    public string Table { get; init; } = string.Empty;
    public string Query { get; init; } = string.Empty;
}
