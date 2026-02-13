using DtPipe.Adapters.Oracle;
using DtPipe.Adapters.PostgreSQL;
using DtPipe.Adapters.SqlServer;
using DtPipe.Adapters.Sqlite;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.MsSql;
using Testcontainers.Oracle;
using Testcontainers.PostgreSql;
using Xunit;
using FluentAssertions;
using System.Security.Cryptography;
using System.Text;

namespace DtPipe.Tests.Integration;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class UniversalQuotingTests : IAsyncLifetime
{
    private string? _postgresConn;
    private string? _sqlServerConn;
    private string? _oracleConn;
    private string _sqlitePath = "";
    private string SqliteConn => $"Data Source={_sqlitePath}";

    private PostgreSqlContainer? _postgresContainer;
    private MsSqlContainer? _sqlServerContainer;
    private OracleContainer? _oracleContainer;

    public async ValueTask InitializeAsync()
    {
        _sqlitePath = Path.Combine(Path.GetTempPath(), $"quoting_test_{Guid.NewGuid()}.db");

        _postgresConn = await DockerHelper.GetPostgreSqlConnectionString(async () =>
        {
            _postgresContainer = new PostgreSqlBuilder("postgres:15-alpine").Build();
            await _postgresContainer.StartAsync();
            return _postgresContainer.GetConnectionString();
        });

        _sqlServerConn = await DockerHelper.GetSqlServerConnectionString(async () =>
        {
            _sqlServerContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
            await _sqlServerContainer.StartAsync();
            return _sqlServerContainer.GetConnectionString();
        });

        _oracleConn = await DockerHelper.GetOracleConnectionString(async () =>
        {
            _oracleContainer = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart").Build();
            await _oracleContainer.StartAsync();
            return _oracleContainer.GetConnectionString();
        });
    }

    public async ValueTask DisposeAsync()
    {
        if (File.Exists(_sqlitePath)) try { File.Delete(_sqlitePath); } catch { }
        if (_postgresContainer != null) await _postgresContainer.DisposeAsync();
        if (_sqlServerContainer != null) await _sqlServerContainer.DisposeAsync();
        if (_oracleContainer != null) await _oracleContainer.DisposeAsync();
    }

    public record QuotingScenario(
        string Provider,
        string TableNameBase, // Base name, will be made unique
        string TableNameSql,  // How we create it in SQL (e.g. \"MixedTable\")
        string TableNameExpected, // How it's stored/referenced (e.g. MixedTable)
        string ColNameSql,
        string ColNameExpected,
        bool ExpectedIsCaseSensitive,
        bool ExpectedNeedsQuoting,
        string ExpectedSafeIdentifier
    )
    {
        private string _uniqueSuffix = "";
        public string GetUniqueTableName(bool useSql)
        {
            if (string.IsNullOrEmpty(_uniqueSuffix))
            {
                using var md5 = MD5.Create();
                var hash = md5.ComputeHash(Encoding.UTF8.GetBytes($"{Provider}_{TableNameBase}_{ColNameExpected}"));
                _uniqueSuffix = Convert.ToHexString(hash).Substring(0, 6);
            }

            var baseName = useSql ? TableNameSql : TableNameExpected;
            // Inject suffix before the closing quote/bracket if any
            if (baseName.EndsWith("\"")) return baseName.Insert(baseName.Length - 1, "_" + _uniqueSuffix);
            if (baseName.EndsWith("]")) return baseName.Insert(baseName.Length - 1, "_" + _uniqueSuffix);
            return baseName + "_" + _uniqueSuffix;
        }

        public override string ToString() => $"{Provider} | {TableNameBase} | {ColNameExpected}";
    }

    public static IEnumerable<object[]> GetScenarios()
    {
        // --- ORACLE ---
        // Normal (UPPER)
        yield return new object[] { new QuotingScenario("oracle", "SIMPLE", "SIMPLE_TAB", "SIMPLE_TAB", "ID", "ID", false, false, "ID") };
        // Mismatch (lower) -> Quotes
        yield return new object[] { new QuotingScenario("oracle", "LOWER", "\"lower_tab\"", "lower_tab", "\"id\"", "id", true, true, "\"id\"") };
        // Mixed -> Quotes
        yield return new object[] { new QuotingScenario("oracle", "MIXED", "\"MixedTab\"", "MixedTab", "\"MixedCol\"", "MixedCol", true, true, "\"MixedCol\"") };
        // Space -> Quotes
        yield return new object[] { new QuotingScenario("oracle", "SPACE", "QUOT_SPACE", "QUOT_SPACE", "\"First Name\"", "First Name", true, true, "\"First Name\"") };
        // Reserved -> Quotes (NOT CaseSensitive because it's UPPER, but NeedsQuoting because it's a reserved word)
        yield return new object[] { new QuotingScenario("oracle", "RESERVED", "RES_WORD", "RES_WORD", "\"ORDER\"", "ORDER", false, true, "\"ORDER\"") };

        // --- POSTGRES ---
        // Normal (lower)
        yield return new object[] { new QuotingScenario("postgres", "simple", "simple_tab", "simple_tab", "id", "id", false, false, "id") };
        // Mismatch (UPPER) -> Quotes
        yield return new object[] { new QuotingScenario("postgres", "UPPER", "\"UPPER_TAB\"", "\"UPPER_TAB\"", "\"ID\"", "ID", true, true, "\"ID\"") };
        // Mixed -> Quotes
        yield return new object[] { new QuotingScenario("postgres", "mixed", "\"MixedTab\"", "\"MixedTab\"", "\"MixedCol\"", "MixedCol", true, true, "\"MixedCol\"") };
        // Space -> Quotes
        yield return new object[] { new QuotingScenario("postgres", "space", "quot_space", "quot_space", "\"First Name\"", "First Name", true, true, "\"First Name\"") };
        // Reserved -> Quotes
        yield return new object[] { new QuotingScenario("postgres", "reserved", "res_word", "res_word", "\"ORDER\"", "ORDER", true, true, "\"ORDER\"") };

        // --- SQL SERVER ---
        yield return new object[] { new QuotingScenario("sqlserver", "Simple", "SimpleTab", "SimpleTab", "Id", "Id", false, false, "Id") };
        yield return new object[] { new QuotingScenario("sqlserver", "Space", "[Quot Space]", "Quot Space", "[First Name]", "First Name", false, true, "[First Name]") };
        yield return new object[] { new QuotingScenario("sqlserver", "Reserved", "ResWord", "ResWord", "[ORDER]", "ORDER", false, true, "[ORDER]") };

        // --- SQLITE ---
        yield return new object[] { new QuotingScenario("sqlite", "Simple", "SimpleTab", "SimpleTab", "Id", "Id", false, false, "Id") };
    }

    [Theory]
    [MemberData(nameof(GetScenarios))]
    public async Task Validate_Quoting_Logic(QuotingScenario s)
    {
        if (s.Provider != "sqlite" && !DockerHelper.IsAvailable()) return;

        var tableNameSql = s.GetUniqueTableName(true);
        var tableNameExpected = s.GetUniqueTableName(false);

        // 1. Create table via raw SQL
        await CreateTableAsync(s.Provider, tableNameSql, s.ColNameSql);

        try
        {
            // 2. Introspect
            var (inspector, dialect) = GetComponents(s.Provider, tableNameExpected);
            var schema = await inspector.InspectTargetAsync();

            schema.Should().NotBeNull();
            schema!.Exists.Should().BeTrue($"Table {tableNameExpected} should exist in {s.Provider}");

            var col = schema.Columns.FirstOrDefault(c => c.Name.Equals(s.ColNameExpected, StringComparison.OrdinalIgnoreCase));
            col.Should().NotBeNull($"Column {s.ColNameExpected} not found in {s.Provider}. Found: {string.Join(", ", schema.Columns.Select(c => c.Name))}");

            // 3. Assertions
            // A. IsCaseSensitive (from Introspection)
            col!.IsCaseSensitive.Should().Be(s.ExpectedIsCaseSensitive, $"IsCaseSensitive mismatch for {s.Provider}.{s.ColNameExpected}");

            // B. NeedsQuoting (from Dialect)
            dialect.NeedsQuoting(col.Name).Should().Be(s.ExpectedNeedsQuoting, $"NeedsQuoting mismatch for {s.Provider}.{s.ColNameExpected}");

            // C. Safe Identifier (Final Output)
            var pipeCol = new PipeColumnInfo(col.Name, typeof(string), true, col.IsCaseSensitive);
            SqlIdentifierHelper.GetSafeIdentifier(dialect, pipeCol).Should().Be(s.ExpectedSafeIdentifier, $"SafeIdentifier mismatch for {s.Provider}.{s.ColNameExpected}");
        }
        finally
        {
            // Cleanup
            await DropTableAsync(s.Provider, tableNameSql);
        }
    }

    private async Task CreateTableAsync(string provider, string tableNameSql, string colNameSql)
    {
        string sql = $"CREATE TABLE {tableNameSql} ({colNameSql} VARCHAR(100))";
        await ExecuteSqlAsync(provider, sql);
    }

    private async Task DropTableAsync(string provider, string tableNameSql)
    {
        string sql = $"DROP TABLE {tableNameSql}";
        try { await ExecuteSqlAsync(provider, sql); } catch { }
    }

    private async Task ExecuteSqlAsync(string provider, string sql)
    {
        switch (provider)
        {
            case "postgres":
                await using (var conn = new NpgsqlConnection(_postgresConn)) { await conn.OpenAsync(); await using (var cmd = new NpgsqlCommand(sql, conn)) await cmd.ExecuteNonQueryAsync(); }
                break;
            case "sqlserver":
                await using (var conn = new SqlConnection(_sqlServerConn)) { await conn.OpenAsync(); await using (var cmd = new SqlCommand(sql, conn)) await cmd.ExecuteNonQueryAsync(); }
                break;
            case "oracle":
                await using (var conn = new OracleConnection(_oracleConn)) { await conn.OpenAsync(); await using (var cmd = new OracleCommand(sql, conn)) await cmd.ExecuteNonQueryAsync(); }
                break;
            case "sqlite":
                await using (var conn = new SqliteConnection(SqliteConn)) { await conn.OpenAsync(); await using (var cmd = new SqliteCommand(sql, conn)) await cmd.ExecuteNonQueryAsync(); }
                break;
        }
    }

    private (ISchemaInspector, ISqlDialect) GetComponents(string provider, string tableNameExpected)
    {
        return provider switch
        {
            "postgres" => (new PostgreSqlDataWriter(_postgresConn!, new PostgreSqlWriterOptions { Table = tableNameExpected }, NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance), new DtPipe.Core.Dialects.PostgreSqlDialect()),
            "sqlserver" => (new SqlServerDataWriter(_sqlServerConn!, new SqlServerWriterOptions { Table = tableNameExpected }, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance), new DtPipe.Core.Dialects.SqlServerDialect()),
            "oracle" => (new OracleDataWriter(_oracleConn!, new OracleWriterOptions { Table = tableNameExpected }, NullLogger<OracleDataWriter>.Instance, OracleTypeConverter.Instance), new DtPipe.Core.Dialects.OracleDialect()),
            "sqlite" => (new SqliteDataWriter(SqliteConn, new SqliteWriterOptions { Table = tableNameExpected }, NullLogger<SqliteDataWriter>.Instance, SqliteTypeConverter.Instance), new DtPipe.Core.Dialects.SqliteDialect()),
            _ => throw new ArgumentException("Unknown provider")
        };
    }
}
