using Microsoft.Data.Sqlite;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's one SQLite database when its state is not in Redis: a single file, <see cref="FileName"/>, in a directory of the
/// host, that the stores borrow and the container disposes. One connection holds the file exclusively for the life of the process, so a
/// second coordinator on the same file is refused at its start instead of writing beside the first. Every write is synced
/// (<c>synchronous=FULL</c>, in WAL), as a journal that never reuses an id needs. A failure of the file is a
/// <see cref="StoreUnavailableException"/>, as an unreachable Redis is.
/// </summary>
public sealed class SqliteDatabase : IDisposable
{
    public const string FileName = "coordinator.db";

    private const int Version = 2;

    private const string Schema = """
        CREATE TABLE IF NOT EXISTS rights (id INTEGER PRIMARY KEY CHECK (id = 1), doc TEXT NOT NULL, version INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS deployments (pipeline TEXT PRIMARY KEY, doc TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cursors (pipeline TEXT NOT NULL, alias TEXT NOT NULL, doc TEXT NOT NULL, PRIMARY KEY (pipeline, alias));
        CREATE TABLE IF NOT EXISTS ignored_hosts (subject TEXT PRIMARY KEY, until_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS session_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, xml TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, doc TEXT NOT NULL, is_over INTEGER NOT NULL, at_ms INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS runs_by_state ON runs (is_over, at_ms);
        CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS owed (run_id TEXT NOT NULL, fragment TEXT NOT NULL, node TEXT NOT NULL, until_ms INTEGER NOT NULL,
            PRIMARY KEY (run_id, fragment, node));
        """;

    private readonly object _gate = new();
    private readonly SqliteConnection _connection;

    private SqliteDatabase(SqliteConnection connection, string path, bool initialize) =>
        (_connection, Path, Initialize) = (connection, path, initialize);

    /// <summary>The file.</summary>
    public string Path { get; }

    /// <summary>Whether a document that is absent may begin: the first start, or after the file was lost.</summary>
    public bool Initialize { get; }

    /// <summary>
    /// Opens, or creates, the database in <paramref name="directory"/> and takes the file. Refuses when another process holds it, when
    /// the file is no database of this server's, and when the directory cannot be written.
    /// </summary>
    public static SqliteDatabase Open(string directory, bool initialize, ILogger logger)
    {
        string path;
        SqliteConnection? connection = null;
        try
        {
            Directory.CreateDirectory(directory);
            path = System.IO.Path.Combine(System.IO.Path.GetFullPath(directory), FileName);
            connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false, DefaultTimeout = 2 }.ToString());
            connection.Open();
            // Exclusive before the first WAL access: the lock is held for the life of the connection and the log needs no shared memory,
            // which a network volume does not offer. A file another process holds is given up after the command timeout, 2 seconds.
            Execute(connection, "PRAGMA busy_timeout = 2000; PRAGMA locking_mode = EXCLUSIVE; PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;");
            var version = (long)ExecuteScalar(connection, "PRAGMA user_version;")!;
            if (version > Version)
                throw new InvalidOperationException($"The coordinator's database ({path}) was written by a newer version of this server.");
            Execute(connection, Schema + $"PRAGMA user_version = {Version};");
        }
        catch (SqliteException ex)
        {
            connection?.Dispose();
            path = System.IO.Path.Combine(directory, FileName);
            throw new InvalidOperationException(ex.SqliteErrorCode is 5 or 6
                ? $"The coordinator's database ({path}) is held by another process: one coordinator runs on a SQLite file. Use Coordinator:Store=redis to run several."
                : ex.SqliteErrorCode == 14
                    ? $"The coordinator could not open its database ({path}): the directory must exist or be creatable, and be writable by the user the server runs as."
                    : $"The coordinator could not open its database ({path}): {ex.Message}", ex);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            connection?.Dispose();
            throw new InvalidOperationException($"The coordinator could not use the directory of its database ({directory}): {ex.Message}", ex);
        }
        catch
        {
            connection?.Dispose();
            throw;
        }
        logger.LogInformation("Coordinator state in SQLite at {Path}", path);
        return new SqliteDatabase(connection, path, initialize);
    }

    /// <summary>
    /// Runs <paramref name="command"/> alone on the connection, in one transaction, and turns a failure of the file into
    /// <see cref="StoreUnavailableException"/>. <paramref name="document"/> is the thing that is kept, in the words of a message.
    /// </summary>
    public T Call<T>(string document, Func<SqliteConnection, SqliteTransaction, T> command)
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
            catch (SqliteException ex)
            {
                throw new StoreUnavailableException($"The coordinator's database ({Path}) failed (SQLite error {ex.SqliteErrorCode}), so {document} may not have been kept.", ex);
            }
        }
    }

    /// <summary>Executes <paramref name="sql"/> with named parameters and says how many rows it changed.</summary>
    public static int Run(SqliteConnection connection, SqliteTransaction? transaction, string sql, params (string Name, object? Value)[] parameters)
    {
        using var command = Prepare(connection, transaction, sql, parameters);
        return command.ExecuteNonQuery();
    }

    /// <summary>The first column of the first row, or null.</summary>
    public static object? Scalar(SqliteConnection connection, SqliteTransaction? transaction, string sql, params (string Name, object? Value)[] parameters)
    {
        using var command = Prepare(connection, transaction, sql, parameters);
        return command.ExecuteScalar();
    }

    /// <summary>Every row, each read by <paramref name="read"/>.</summary>
    public static List<T> Rows<T>(SqliteConnection connection, SqliteTransaction? transaction, string sql, Func<SqliteDataReader, T> read, params (string Name, object? Value)[] parameters)
    {
        using var command = Prepare(connection, transaction, sql, parameters);
        using var reader = command.ExecuteReader();
        var rows = new List<T>();
        while (reader.Read()) rows.Add(read(reader));
        return rows;
    }

    public void Dispose() => _connection.Dispose();

    private static SqliteCommand Prepare(SqliteConnection connection, SqliteTransaction? transaction, string sql, (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var (name, value) in parameters) command.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return command;
    }

    private static void Execute(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static object? ExecuteScalar(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }
}
