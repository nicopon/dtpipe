namespace DtPipe.Core.Abstractions;

/// <summary>
/// Indicates that the component uses a specific SQL dialect.
/// </summary>
public interface IHasSqlDialect
{
    ISqlDialect Dialect { get; }
}
