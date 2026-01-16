namespace QueryDump.Core.Options;

/// <summary>
/// Base interface for any option set.
/// </summary>
public interface IOptionSet
{
    /// <summary>
    /// The CLI prefix for this option set (e.g. "ora", "csv").
    /// </summary>
    static abstract string Prefix { get; }
}

/// <summary>
/// Options specific to a data provider (Oracle, SQL Server, etc.).
/// </summary>
public interface IProviderOptions : IOptionSet { }

/// <summary>
/// Options specific to a data writer (CSV, Parquet, etc.).
/// </summary>
public interface IWriterOptions : IOptionSet { }

/// <summary>
/// Options specific to a data transformer (Faker, etc.).
/// </summary>
public interface ITransformerOptions : IOptionSet { }
