using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's state in a SQLite file, <see cref="FileName"/>, in a directory of the host. The connection holds the file
/// exclusively for the life of the process, so a second coordinator on the same file is refused at its start instead of writing beside
/// the first. Every write is synced (<c>synchronous=FULL</c>, in WAL), as a journal that never reuses an id needs.
/// </summary>
internal sealed class SqliteDialect(string directory) : StoreDialect
{
    public const string FileName = "coordinator.db";

    private readonly string _directory = directory;

    /// <summary>The file.</summary>
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetFullPath(directory), FileName);

    public override string Name => "SQLite";

    public override string Where => $"SQLite, {Path}";

    public override bool CanBeLost => false;

    public override string Schema => """
        CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS rights (id INTEGER PRIMARY KEY CHECK (id = 1), doc TEXT NOT NULL, version INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS deployments (pipeline TEXT PRIMARY KEY, doc TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cursors (pipeline TEXT NOT NULL, alias TEXT NOT NULL, doc TEXT NOT NULL, PRIMARY KEY (pipeline, alias));
        CREATE TABLE IF NOT EXISTS ignored_hosts (subject TEXT PRIMARY KEY, until_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS session_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, xml TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS issuer_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, xml TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, doc TEXT NOT NULL, is_over INTEGER NOT NULL, at_ms INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS runs_by_state ON runs (is_over, at_ms);
        CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS owed (run_id TEXT NOT NULL, fragment TEXT NOT NULL, node TEXT NOT NULL, until_ms INTEGER NOT NULL,
            PRIMARY KEY (run_id, fragment, node));
        """;

    public override string Upsert(string table, string[] keys, string[] values) => OnConflict(table, keys, values);

    public override string First(string count) => $"LIMIT {count}";

    /// <summary>Null when another process holds the file; refuses when the file is no database and when the directory cannot be written.</summary>
    public override DbConnection? TryConnect()
    {
        SqliteConnection? connection = null;
        try
        {
            Directory.CreateDirectory(_directory);
            connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = Path, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false, DefaultTimeout = 2 }.ToString());
            connection.Open();
            // Exclusive before the first WAL access: the lock is held for the life of the connection and the log needs no shared memory,
            // which a network volume does not offer. The first read takes the file, here and not at the first store's call. A file another
            // process holds is given up after the command timeout, 2 seconds.
            using var command = connection.CreateCommand();
            command.CommandText = "PRAGMA busy_timeout = 2000; PRAGMA locking_mode = EXCLUSIVE; PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;";
            command.ExecuteNonQuery();
            // A scalar, since a non-query does not step a SELECT.
            command.CommandText = "SELECT COUNT(*) FROM sqlite_master;";
            command.ExecuteScalar();
            return connection;
        }
        catch (SqliteException ex)
        {
            connection?.Dispose();
            if (ex.SqliteErrorCode is 5 or 6) return null;
            throw new InvalidOperationException(ex.SqliteErrorCode == 14
                ? $"The coordinator could not open its database ({Path}): the directory must exist or be creatable, and be writable by the user the server runs as."
                : $"The coordinator could not open its database ({Path}): {ex.Message}", ex);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            connection?.Dispose();
            throw new InvalidOperationException($"The coordinator could not use the directory of its database ({_directory}): {ex.Message}", ex);
        }
        catch
        {
            connection?.Dispose();
            throw;
        }
    }

    public override string Describe(DbException exception) =>
        exception is SqliteException sqlite ? $"SQLite error {sqlite.SqliteErrorCode}" : exception.GetType().Name;
}
