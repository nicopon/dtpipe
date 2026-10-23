namespace DtPipe.Core.Abstractions;

/// <summary>
/// Optional interface for data writers that support primary-key validation, so dry run can check
/// a key specification before the pipeline executes. Implemented alongside
/// <see cref="ISchemaInspector"/> by writers offering key-based strategies.
/// </summary>
public interface IKeyValidator
{
    /// <summary>
    /// The write strategy name, as a string (e.g. "Upsert"), or null when not applicable.
    /// Determines whether a primary key is required.
    /// </summary>
    string? GetWriteStrategy();

    /// <summary>
    /// The key column names as the user supplied them (e.g. via --key), or null if none were given.
    /// <para>
    /// Must return the RAW input, not resolved or normalized names: resolution and validation belong
    /// to the dry-run analyzer, which cannot check what a writer has already rewritten. "Id,Name"
    /// yields ["Id", "Name"].
    /// </para>
    /// </summary>
    IReadOnlyList<string>? GetRequestedPrimaryKeys();

    /// <summary>
    /// True when the current strategy cannot work without a primary key — Upsert and Ignore, which
    /// need one to detect a conflicting row. Append, Truncate, DeleteThenInsert and Recreate do not.
    /// </summary>
    bool RequiresPrimaryKey();
}
