using DtPipe.Tests.Helpers;
using Testcontainers.MsSql;
using Testcontainers.Oracle;
using Testcontainers.PostgreSql;
using Xunit;

namespace DtPipe.Tests.Fixtures;

public class GlobalDatabaseFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _postgres;
    private MsSqlContainer? _sqlServer;
    private OracleContainer? _oracle;

    public string? PostgresConnectionString { get; private set; }
    public string? SqlServerConnectionString { get; private set; }
    public string? OracleConnectionString { get; private set; }

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable()) return;

        var tasks = new List<Task>();

        // Postgres
        tasks.Add(Task.Run(async () =>
        {
            _postgres = new PostgreSqlBuilder("postgres:15-alpine")
                .Build();
            await _postgres.StartAsync();
            PostgresConnectionString = _postgres.GetConnectionString();
        }));

        // SQL Server
        tasks.Add(Task.Run(async () =>
        {
            _sqlServer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
                .Build();
            await _sqlServer.StartAsync();
            SqlServerConnectionString = _sqlServer.GetConnectionString();
        }));

        // Oracle
        tasks.Add(Task.Run(async () =>
        {
            _oracle = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart")
                .Build();
            await _oracle.StartAsync();
            OracleConnectionString = _oracle.GetConnectionString();
        }));

        await Task.WhenAll(tasks);
    }

    public async ValueTask DisposeAsync()
    {
        var tasks = new List<Task>();

        if (_postgres != null) tasks.Add(_postgres.DisposeAsync().AsTask());
        if (_sqlServer != null) tasks.Add(_sqlServer.DisposeAsync().AsTask());
        if (_oracle != null) tasks.Add(_oracle.DisposeAsync().AsTask());

        await Task.WhenAll(tasks);
    }
}
