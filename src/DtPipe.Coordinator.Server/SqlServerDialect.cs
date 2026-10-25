using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's state in a SQL Server database of its own, which <c>Coordinator:ConnectionString</c> names. The connection is out
/// of the pool, never resumed by the driver (<c>ConnectRetryCount=0</c>: a session it opened again would hold no lock), and holds an
/// application lock of the session (<see cref="LockName"/>) for the life of the process. The keys are compared in a binary collation,
/// as the other providers compare them, and are bounded to fit an index key.
/// </summary>
internal sealed class SqlServerDialect(string connectionString) : StoreDialect
{
    /// <summary>The application lock of the database. A resource of the database, so one coordinator per database.</summary>
    public const string LockName = "dtpipe-coordinator";

    private const string Key = "COLLATE Latin1_General_100_BIN2 NOT NULL";

    public override string Name => "SQL Server";

    public override string Where => "SQL Server, Coordinator:ConnectionString";

    public override bool CanBeLost => true;

    public override string Schema => $"""
        IF OBJECT_ID(N'schema_version', N'U') IS NULL CREATE TABLE schema_version (version bigint NOT NULL);
        IF OBJECT_ID(N'rights', N'U') IS NULL CREATE TABLE rights (id int PRIMARY KEY CHECK (id = 1), doc nvarchar(max) NOT NULL, version bigint NOT NULL);
        IF OBJECT_ID(N'deployments', N'U') IS NULL CREATE TABLE deployments (pipeline nvarchar(64) {Key} PRIMARY KEY, doc nvarchar(max) NOT NULL);
        IF OBJECT_ID(N'cursors', N'U') IS NULL CREATE TABLE cursors (pipeline nvarchar(64) {Key}, alias nvarchar(256) {Key}, doc nvarchar(max) NOT NULL,
            PRIMARY KEY (pipeline, alias));
        IF OBJECT_ID(N'ignored_hosts', N'U') IS NULL CREATE TABLE ignored_hosts (subject nvarchar(128) {Key} PRIMARY KEY, until_ms bigint NOT NULL);
        IF OBJECT_ID(N'session_keys', N'U') IS NULL CREATE TABLE session_keys (id bigint IDENTITY(1,1) PRIMARY KEY, xml nvarchar(max) NOT NULL);
        IF OBJECT_ID(N'issuer_keys', N'U') IS NULL CREATE TABLE issuer_keys (id bigint IDENTITY(1,1) PRIMARY KEY, xml nvarchar(max) NOT NULL);
        IF OBJECT_ID(N'runs', N'U') IS NULL CREATE TABLE runs (id nvarchar(64) {Key} PRIMARY KEY, doc nvarchar(max) NOT NULL, is_over int NOT NULL,
            at_ms bigint NOT NULL);
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'runs_by_state' AND object_id = OBJECT_ID(N'runs')) CREATE INDEX runs_by_state ON runs (is_over, at_ms);
        IF OBJECT_ID(N'counters', N'U') IS NULL CREATE TABLE counters (name nvarchar(64) {Key} PRIMARY KEY, value bigint NOT NULL);
        IF OBJECT_ID(N'owed', N'U') IS NULL CREATE TABLE owed (run_id nvarchar(64) {Key}, fragment nvarchar(256) {Key}, node nvarchar(64) {Key},
            until_ms bigint NOT NULL, PRIMARY KEY (run_id, fragment, node));
        """;

    public override string Upsert(string table, string[] keys, string[] values)
    {
        var columns = keys.Concat(values).ToArray();
        return $"MERGE {table} WITH (HOLDLOCK) AS t USING (VALUES ({string.Join(", ", columns.Select(c => "@" + c))})) AS s ({string.Join(", ", columns)}) "
            + $"ON {string.Join(" AND ", keys.Select(c => $"t.{c} = s.{c}"))} "
            + $"WHEN MATCHED THEN UPDATE SET {string.Join(", ", values.Select(c => $"{c} = s.{c}"))} "
            + $"WHEN NOT MATCHED THEN INSERT ({string.Join(", ", columns)}) VALUES ({string.Join(", ", columns.Select(c => "s." + c))});";
    }

    public override string First(string count) => $"OFFSET 0 ROWS FETCH NEXT {count} ROWS ONLY";

    public override DbConnection? TryConnect()
    {
        SqlConnectionStringBuilder builder;
        try
        {
            builder = new SqlConnectionStringBuilder(connectionString) { Pooling = false, ConnectRetryCount = 0 };
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or KeyNotFoundException)
        {
            throw new InvalidOperationException("Coordinator:ConnectionString is not a valid SQL Server connection string.");
        }
        var connection = new SqlConnection(builder.ConnectionString);
        try
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "DECLARE @r int; EXEC @r = sp_getapplock @Resource = @name, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT @r;";
            command.Parameters.AddWithValue("@name", LockName);
            var granted = Convert.ToInt32(command.ExecuteScalar());
            if (granted >= 0) return connection;
            connection.Dispose();
            // -1: another session holds it; anything else is a refusal of the server.
            return granted == -1 ? null
                : throw new InvalidOperationException($"The coordinator could not take its database ({Where}): sp_getapplock answered {granted}.");
        }
        catch (Exception ex) when (ex is DbException or TimeoutException or OperationCanceledException)
        {
            connection.Dispose();
            throw new InvalidOperationException(
                $"The coordinator could not open its database ({Where}): {(ex is DbException db ? Describe(db) : "timed out")}.");
        }
    }

    public override string Describe(DbException exception) =>
        exception is SqlException sql ? $"SQL Server error {sql.Number}" : $"SQL Server, {exception.GetType().Name}";
}
