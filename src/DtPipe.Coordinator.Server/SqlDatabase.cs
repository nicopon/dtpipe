using System.Data.Common;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's one database when its state is not in Redis, on the provider its <see cref="StoreDialect"/> speaks: one connection
/// for the life of the process, which the stores borrow and the container disposes, and on which every read and write runs, one at a
/// time. A failure of the database, whatever form the driver gives it, is a <see cref="StoreUnavailableException"/>, as an unreachable
/// Redis is.
/// </summary>
internal sealed class SqlDatabase : IDisposable
{
    private const int Version = 1;

    private readonly object _gate = new();
    private readonly DbConnection _connection;

    private SqlDatabase(StoreDialect dialect, DbConnection connection, bool initialize) =>
        (Dialect, _connection, Initialize) = (dialect, connection, initialize);

    /// <summary>What this provider writes differently.</summary>
    public StoreDialect Dialect { get; }

    /// <summary>Whether a document that is absent may begin: the first start, or after the database was lost.</summary>
    public bool Initialize { get; }

    /// <summary>
    /// Takes the database (<see cref="StoreDialect.Connect"/>) and creates what it lacks. Refuses a database that a newer version of
    /// this server wrote.
    /// </summary>
    public static SqlDatabase Open(StoreDialect dialect, bool initialize, ILogger logger)
    {
        var database = new SqlDatabase(dialect, dialect.Connect(), initialize);
        try
        {
            database.Call("the schema", (c, t) =>
            {
                Run(c, t, dialect.Schema);
                var version = Scalar(c, t, "SELECT MAX(version) FROM schema_version");
                if (version is null or DBNull) return Run(c, t, "INSERT INTO schema_version (version) VALUES (@version)", ("@version", Version));
                return Convert.ToInt64(version) <= Version ? 0
                    : throw new InvalidOperationException($"The coordinator's database ({dialect.Where}) was written by a newer version of this server.");
            });
        }
        catch
        {
            database.Dispose();
            throw;
        }
        logger.LogInformation("Coordinator state in {Provider} at {Where}", dialect.Name, dialect.Where);
        return database;
    }

    /// <summary>
    /// Runs <paramref name="command"/> alone on the connection, in one transaction, and turns a failure of the database into
    /// <see cref="StoreUnavailableException"/>: an error of the driver, its expiry and its cancellation alike. What the command throws
    /// on its own account passes. <paramref name="document"/> is the thing that is kept, in the words of a message.
    /// </summary>
    public T Call<T>(string document, Func<DbConnection, DbTransaction, T> command)
    {
        lock (_gate)
        {
            try
            {
                using var transaction = _connection.BeginTransaction();
                var result = command(_connection, transaction);
                transaction.Commit();
                return result;
            }
            catch (Exception ex) when (ex is DbException or OperationCanceledException or TimeoutException)
            {
                // No inner exception: a driver's message may name the host of a connection string, and a log would write it whole.
                var failure = ex switch
                {
                    DbException db => Dialect.Describe(db),
                    OperationCanceledException => "cancelled",
                    _ => "timed out",
                };
                throw new StoreUnavailableException($"The coordinator's database ({Dialect.Where}) failed ({failure}), so {document} may not have been kept.");
            }
        }
    }

    /// <summary>Executes <paramref name="sql"/> with named parameters and says how many rows it changed.</summary>
    public static int Run(DbConnection connection, DbTransaction? transaction, string sql, params (string Name, object? Value)[] parameters)
    {
        using var command = Prepare(connection, transaction, sql, parameters);
        return command.ExecuteNonQuery();
    }

    /// <summary>The first column of the first row, or null.</summary>
    public static object? Scalar(DbConnection connection, DbTransaction? transaction, string sql, params (string Name, object? Value)[] parameters)
    {
        using var command = Prepare(connection, transaction, sql, parameters);
        return command.ExecuteScalar();
    }

    /// <summary>Every row, each read by <paramref name="read"/>.</summary>
    public static List<T> Rows<T>(DbConnection connection, DbTransaction? transaction, string sql, Func<DbDataReader, T> read, params (string Name, object? Value)[] parameters)
    {
        using var command = Prepare(connection, transaction, sql, parameters);
        using var reader = command.ExecuteReader();
        var rows = new List<T>();
        while (reader.Read()) rows.Add(read(reader));
        return rows;
    }

    public void Dispose() => _connection.Dispose();

    private static DbCommand Prepare(DbConnection connection, DbTransaction? transaction, string sql, (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }
        return command;
    }
}
