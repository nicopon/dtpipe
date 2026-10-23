namespace DtPipe.Core.Abstractions;

using DtPipe.Core.Models;

/// <summary>
/// Defines dialect-specific behaviors for SQL generation, particularly regarding identifier casing and quoting.
/// </summary>
public interface ISqlDialect
{
    /// <summary>
    /// Normalizes an identifier according to the database's default casing rules.
    /// e.g. "MyTable" -> "mytable" (Postgres), "MYTABLE" (Oracle), "MyTable" (SQL Server/SQLite).
    /// </summary>
    string Normalize(string identifier);

    /// <summary>
    /// Quotes an identifier to preserve case and handle special characters.
    /// e.g. "MyTable" -> "\"MyTable\"" (Postgres/Oracle), "[MyTable]" (SQL Server).
    /// </summary>
    string Quote(string identifier);

    /// <summary>
    /// Determines whether an identifier needs quoting based on the dialect's rules and the input string.
    /// Use this to implement "Smart Quoting".
    /// </summary>
    bool NeedsQuoting(string identifier);

    /// <summary>
    /// Gets the SQL query used to discover available tables and views in this dialect's database, or null if unsupported.
    /// </summary>
    string? TableDiscoveryQuery => null;

    /// <summary>
    /// SQL that puts the session in read-only mode, or <c>null</c> when the engine has no
    /// equivalent.
    ///
    /// This is the strong form of sample-mode safety: the SERVER refuses a write, rather than a
    /// verb scan guessing whether a query might perform one. A scan cannot prove a query is
    /// read-only — <c>SELECT my_function()</c> passes it — so where the engine can enforce the
    /// property, it should.
    ///
    /// A dialect that returns null is not a gap to paper over: the sample report says which of
    /// the two it got, because a guarantee that is sometimes absent must never be reported as
    /// though it were always present.
    /// </summary>
    string? ReadOnlySessionSql => null;

    /// <summary>
    /// F9 — builds the staged-merge SQL for this dialect from a shared spec.
    /// Base implementation emits ANSI INSERT … ON CONFLICT; dialects override for MERGE
    /// syntax or constraint fallbacks (';'-separated multi-step scripts allowed).
    /// </summary>
    string BuildStagingMerge(MergeSpec spec);

    /// <summary>
    /// F9 — conflict clause for parameterized inserts (SQLite-style VALUES batches).
    /// Empty when unsupported.
    /// </summary>
    string BuildParameterizedConflictClause(IReadOnlyList<string> rawKeys, IReadOnlyList<PipeColumnInfo> columns) => "";
}
