using System.Data.Common;
using DtPipe.Coordinator.Server.Stores;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A database of its own, on the provider a test names, and the starts of a coordinator over it. A start ends the earlier one, since
/// one coordinator holds the database at a time. SQLite is a file in the directory the test gives, which the test removes. PostgreSQL
/// and SQL Server need a server: <c>DTPIPE_TEST_POSTGRES</c> and <c>DTPIPE_TEST_SQLSERVER</c> name one (a connection string whose user
/// may create a database), and <c>DTPIPE_TEST_REUSE_INFRA</c> stands for the containers of <c>tests/infra</c>; the bed creates a
/// database per test there and drops it when disposed.
/// </summary>
internal sealed class SqlTestBed : IDisposable
{
    /// <summary>The providers the suite of the stores runs on.</summary>
    public static readonly string[] Names = ["sqlite", "postgresql", "sqlserver"];

    public static TheoryData<string> Providers => [.. Names];

    private const string InfraPostgres = "Host=localhost;Port=5440;Username=postgres;Password=password;Database=postgres";
    private const string InfraSqlServer = "Server=localhost,1434;User Id=sa;Password=Password123!;TrustServerCertificate=true;Encrypt=false";

    private readonly string _provider;
    private readonly string _directory;
    private readonly List<string> _databases = [];
    private readonly List<SqlDatabase> _others = [];
    private SqlDatabase? _open;

    public SqlTestBed(string provider, string directory)
    {
        (_provider, _directory) = (provider, directory);
        if (provider != "sqlite")
            Assert.SkipUnless(Server(provider) is not null,
                $"No {provider}: set DTPIPE_TEST_{(provider == "postgresql" ? "POSTGRES" : "SQLSERVER")}, or DTPIPE_TEST_REUSE_INFRA=true with tests/infra up.");
        ConnectionString = provider == "sqlite" ? null : Create();
    }

    /// <summary>The database of the bed, for a server's provider.</summary>
    public string? ConnectionString { get; }

    /// <summary>One start of the coordinator: the earlier one is over.</summary>
    public SqlDatabase Start(bool initialize = true)
    {
        _open?.Dispose();
        _open = SqlDatabase.Open(Dialect(), initialize, TimeSpan.Zero, NullLogger.Instance);
        return _open;
    }

    /// <summary>A start of another coordinator, over a database of its own, beside this bed's.</summary>
    public SqlDatabase StartOther()
    {
        var database = SqlDatabase.Open(_provider == "sqlite" ? new SqliteDialect(Path.Combine(_directory, "other-" + _others.Count)) : DialectOf(Create()),
            true, TimeSpan.Zero, NullLogger.Instance);
        _others.Add(database);
        return database;
    }

    /// <summary>The dialect over the bed's database, for a test that opens it on its own terms.</summary>
    public StoreDialect Dialect() => _provider == "sqlite" ? new SqliteDialect(_directory) : DialectOf(ConnectionString!);

    /// <summary>Runs <paramref name="sql"/> on the database outside the coordinator, which is stopped first.</summary>
    public void Execute(string sql)
    {
        _open?.Dispose();
        _open = null;
        using var raw = Raw(_provider == "sqlite" ? $"Data Source={Path.Combine(_directory, SqliteDialect.FileName)};Pooling=False" : ConnectionString!);
        Run(raw, sql);
    }

    /// <summary>The first column of the first row <paramref name="sql"/> gives, read from another session while the coordinator runs.</summary>
    public object? Read(string sql)
    {
        using var raw = Raw(_provider == "sqlite" ? $"Data Source={Path.Combine(_directory, SqliteDialect.FileName)};Pooling=False" : ConnectionString!);
        using var command = raw.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    /// <summary>Ends, from another session, every session but this one on the bed's database: what an administrator or a failover does.</summary>
    public void CutSessions()
    {
        using var admin = Raw(Server(_provider)!);
        var name = DatabaseOf(ConnectionString!);
        Run(admin, _provider == "postgresql"
            ? $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{name}' AND pid <> pg_backend_pid()"
            : $"DECLARE @k nvarchar(max) = N''; SELECT @k += N'KILL ' + CAST(session_id AS nvarchar(10)) + N'; ' FROM sys.dm_exec_sessions "
              + $"WHERE database_id = DB_ID(N'{name}') AND session_id <> @@SPID; EXEC (@k);");
    }

    public void Dispose()
    {
        _open?.Dispose();
        foreach (var database in _others) database.Dispose();
        SqliteConnection.ClearAllPools();
        if (_databases.Count == 0) return;
        using var admin = Raw(Server(_provider)!);
        foreach (var name in _databases)
            Run(admin, _provider == "postgresql"
                ? $"DROP DATABASE IF EXISTS {name} WITH (FORCE)"
                : $"IF DB_ID(N'{name}') IS NOT NULL BEGIN ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{name}]; END");
    }

    private static string? Server(string provider) =>
        Environment.GetEnvironmentVariable(provider == "postgresql" ? "DTPIPE_TEST_POSTGRES" : "DTPIPE_TEST_SQLSERVER") is { Length: > 0 } named ? named
        : Environment.GetEnvironmentVariable("DTPIPE_TEST_REUSE_INFRA") is "true" or "1" ? provider == "postgresql" ? InfraPostgres : InfraSqlServer
        : null;

    // A database of its own, since the lock and the tables are the database's.
    private string Create()
    {
        var name = "coordinator_test_" + Guid.NewGuid().ToString("N")[..16];
        using (var admin = Raw(Server(_provider)!)) Run(admin, $"CREATE DATABASE {name}");
        _databases.Add(name);
        return _provider == "postgresql"
            ? new NpgsqlConnectionStringBuilder(Server(_provider)) { Database = name }.ConnectionString
            : new SqlConnectionStringBuilder(Server(_provider)) { InitialCatalog = name }.ConnectionString;
    }

    private StoreDialect DialectOf(string connectionString) =>
        _provider == "postgresql" ? new PostgreSqlDialect(connectionString) : new SqlServerDialect(connectionString);

    private string DatabaseOf(string connectionString) =>
        _provider == "postgresql" ? new NpgsqlConnectionStringBuilder(connectionString).Database! : new SqlConnectionStringBuilder(connectionString).InitialCatalog;

    private DbConnection Raw(string connectionString)
    {
        DbConnection connection = _provider switch
        {
            "sqlite" => new SqliteConnection(connectionString),
            "postgresql" => new NpgsqlConnection(new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString),
            _ => new SqlConnection(new SqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString),
        };
        connection.Open();
        return connection;
    }

    private static void Run(DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
