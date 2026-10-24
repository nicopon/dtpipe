using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A database of its own, on the provider a test names, and the starts of a coordinator over it. A start ends the earlier one, since
/// one coordinator holds the database at a time. SQLite is a file in the directory the test gives, which the test removes.
/// </summary>
internal sealed class SqlTestBed(string provider, string directory) : IDisposable
{
    /// <summary>The providers the suite of the stores runs on.</summary>
    public static readonly string[] Names = ["sqlite"];

    public static TheoryData<string> Providers => [.. Names];

    private SqlDatabase? _open;
    private readonly List<SqlDatabase> _others = [];

    /// <summary>One start of the coordinator: the earlier one is over.</summary>
    public SqlDatabase Start(bool initialize = true)
    {
        _open?.Dispose();
        _open = SqlDatabase.Open(Dialect(directory), initialize, NullLogger.Instance);
        return _open;
    }

    /// <summary>A start of another coordinator, over a database of its own, beside this bed's.</summary>
    public SqlDatabase StartOther()
    {
        var database = SqlDatabase.Open(Dialect(Path.Combine(directory, "other-" + _others.Count)), true, NullLogger.Instance);
        _others.Add(database);
        return database;
    }

    /// <summary>Runs <paramref name="sql"/> on the database outside the coordinator, which is stopped first.</summary>
    public void Execute(string sql)
    {
        _open?.Dispose();
        _open = null;
        using var raw = new SqliteConnection($"Data Source={Path.Combine(directory, SqliteDialect.FileName)};Pooling=False");
        raw.Open();
        using var command = raw.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _open?.Dispose();
        foreach (var database in _others) database.Dispose();
        SqliteConnection.ClearAllPools();
    }

    private StoreDialect Dialect(string at) => provider switch
    {
        "sqlite" => new SqliteDialect(at),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "no such provider"),
    };
}
