using System.Data.Common;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What differs between the databases the coordinator's state can be kept in, and nothing else: the schema, an upsert, the clause
/// that keeps the first rows of an ordered query, how the one connection is opened, and how a failure of the driver is told. Every
/// other statement the stores run is the same text on each, with <c>@name</c> parameters: a statement that needs a fourth point
/// of variation is written in the portable form instead (the run counter has a single writer, so it needs no upsert).
/// </summary>
internal abstract class StoreDialect
{
    /// <summary>The provider, in the words of a message.</summary>
    public abstract string Name { get; }

    /// <summary>Where the state is, in the words of a message: a path, or the setting that names the database, never its text.</summary>
    public abstract string Where { get; }

    /// <summary>
    /// The tables, each created when absent, so that running it again changes nothing; <c>schema_version</c> among them, which
    /// <see cref="SqlDatabase"/> reads.
    /// </summary>
    public abstract string Schema { get; }

    /// <summary>
    /// A statement that writes the row of <paramref name="table"/> whose key is <paramref name="keys"/>, and replaces its
    /// <paramref name="values"/> when the key is there; each column takes the parameter of its name.
    /// </summary>
    public abstract string Upsert(string table, string[] keys, string[] values);

    /// <summary>The clause, after an <c>ORDER BY</c>, that keeps the first rows, as many as the parameter <paramref name="count"/> says.</summary>
    public abstract string First(string count);

    /// <summary>
    /// Opens the one connection of the process and takes the database for this coordinator, or refuses naming why. A refusal is an
    /// <see cref="InvalidOperationException"/> that names the provider and <see cref="Where"/>, never a part of a connection string.
    /// </summary>
    public abstract DbConnection Connect();

    /// <summary>A failure of the driver, in the words of a message: its code, never its message, which may name the host.</summary>
    public abstract string Describe(DbException exception);

    /// <summary>The <c>ON CONFLICT</c> form of <see cref="Upsert"/>, which SQLite and PostgreSQL share.</summary>
    protected static string OnConflict(string table, string[] keys, string[] values) =>
        $"INSERT INTO {table} ({string.Join(", ", keys.Concat(values))}) VALUES ({string.Join(", ", keys.Concat(values).Select(c => "@" + c))}) "
        + $"ON CONFLICT ({string.Join(", ", keys)}) DO UPDATE SET {string.Join(", ", values.Select(c => $"{c} = excluded.{c}"))}";
}
