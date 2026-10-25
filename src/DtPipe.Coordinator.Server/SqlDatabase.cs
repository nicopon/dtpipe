using System.Data.Common;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's one database, on the provider its <see cref="StoreDialect"/> speaks: one connection
/// for the life of the process, which holds the database for this coordinator, which the stores borrow and the container disposes, and
/// on which every read and write runs, one at a time. It never opens a second one: a session opened again would hold no lock. A
/// database that can be lost is probed; lost, it says so once, every call after fails, and it calls the action it was given, which
/// stops the coordinator, since another may now take the database. A failure of the database, whatever form the driver gives it, is a
/// <see cref="StoreUnavailableException"/>.
/// </summary>
internal sealed class SqlDatabase : IDisposable
{
    private const int Version = 1;

    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(1);

    private readonly object _gate = new();
    private readonly DbConnection _connection;
    private readonly ILogger _logger;
    private readonly Action _lost;
    private readonly Timer? _probe;
    private bool _isLost;

    private SqlDatabase(StoreDialect dialect, DbConnection connection, bool initialize, ILogger logger, Action lost, TimeSpan probeEvery)
    {
        (Dialect, _connection, Initialize, _logger, _lost) = (dialect, connection, initialize, logger, lost);
        if (dialect.CanBeLost) _probe = new Timer(_ => Probe(), null, probeEvery, probeEvery);
    }

    /// <summary>What this provider writes differently.</summary>
    public StoreDialect Dialect { get; }

    /// <summary>Whether a document that is absent may begin: the first start, or after the database was lost.</summary>
    public bool Initialize { get; }

    /// <summary>
    /// Takes the database (<see cref="StoreDialect.TryConnect"/>), trying each second for <paramref name="wait"/> while another
    /// coordinator holds it, then refusing naming the provider, or without bound when <paramref name="wait"/> is null (a passive
    /// standby); and creates what it lacks. The connection that took the database is the one it keeps: no try leaves one behind, and
    /// none is let go and taken again. <paramref name="stopping"/> ends the wait with an <see cref="OperationCanceledException"/>,
    /// nothing taken. Refuses a database that a newer version of this server wrote. <paramref name="lost"/> is called once, off the
    /// caller's thread, if the database is lost; the probe runs every <paramref name="probeEvery"/>, 5 seconds unless given.
    /// </summary>
    public static SqlDatabase Open(StoreDialect dialect, bool initialize, TimeSpan? wait, ILogger logger, Action? lost = null, TimeSpan? probeEvery = null,
        CancellationToken stopping = default)
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        var held = false;
        DbConnection? connection;
        while ((connection = dialect.TryConnect()) is null)
        {
            if (wait is { } bound && waited.Elapsed >= bound)
                throw new InvalidOperationException(
                    $"The coordinator's database ({dialect.Where}) is held by another coordinator, still after Coordinator:StoreLockWait ({bound.TotalSeconds:0.#} s): one coordinator runs on a database.");
            if (!held)
            {
                if (wait is { } limit)
                    logger.LogWarning("Another coordinator holds the database ({Where}): waiting up to {Wait} s for it to leave (Coordinator:StoreLockWait)", dialect.Where, limit.TotalSeconds);
                else
                    logger.LogWarning("Another coordinator holds the database ({Where}): this one is passive (Coordinator:Standby) and waits for it to leave, without bound", dialect.Where);
                held = true;
            }
            stopping.WaitHandle.WaitOne(Retry);
            stopping.ThrowIfCancellationRequested();
        }
        if (held && wait is null) logger.LogWarning("The database ({Where}) is this coordinator's now: it leaves the passive state and starts", dialect.Where);
        var database = new SqlDatabase(dialect, connection, initialize, logger, lost ?? (() => { }), probeEvery ?? TimeSpan.FromSeconds(5));
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
        logger.LogInformation("Coordinator state in {Where}", dialect.Where);
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
            if (_isLost || _connection.State != System.Data.ConnectionState.Open)
            {
                Lose();
                throw new StoreUnavailableException($"The coordinator's database ({Dialect.Where}) was lost, so {document} was not kept.");
            }
            try
            {
                using var transaction = _connection.BeginTransaction();
                var result = command(_connection, transaction);
                transaction.Commit();
                return result;
            }
            catch (Exception ex) when (ex is DbException or OperationCanceledException or TimeoutException)
            {
                if (_connection.State != System.Data.ConnectionState.Open) Lose();
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

    public void Dispose()
    {
        _probe?.Dispose();
        lock (_gate) _connection.Dispose();
    }

    // A tick that finds a call under way skips: that call reads the connection as well.
    private void Probe()
    {
        if (!Monitor.TryEnter(_gate)) return;
        try
        {
            if (_isLost) return;
            if (_connection.State == System.Data.ConnectionState.Open)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.ExecuteScalar();
                if (_connection.State == System.Data.ConnectionState.Open) return;
            }
            Lose();
        }
        // A probe that fails on a session still open (a slow server) holds the lock still.
        catch (Exception ex) when (ex is DbException or InvalidOperationException or OperationCanceledException or TimeoutException)
        {
            if (_connection.State != System.Data.ConnectionState.Open) Lose();
        }
        finally
        {
            Monitor.Exit(_gate);
        }
    }

    // Under the gate. Once: the coordinator stops, and every later call fails.
    private void Lose()
    {
        if (_isLost) return;
        _isLost = true;
        _logger.LogCritical("The coordinator lost its database ({Where}) and the lock it held: another coordinator may take it, so this one stops", Dialect.Where);
        ThreadPool.QueueUserWorkItem(_ => _lost());
    }

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
