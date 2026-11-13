namespace DtPipe.Core.Abstractions;

/// <summary>
/// A component that executes SQL it holds itself rather than in a reader's options, such as a stream
/// processor's query and init script. A sample run classifies these values, as written, before the
/// component opens; a processor that does not declare them runs SQL the sample gate never sees.
/// </summary>
public interface ISqlBearing
{
    /// <summary>The SQL values as written: an <c>@file</c>, <c>${{NAME}}</c> or <c>keyring://</c> reference unresolved.</summary>
    IEnumerable<string?> SqlBearingValues { get; }
}
